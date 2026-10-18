import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:http/http.dart' as http;
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';

/// User-configured endpoint that receives finished transcripts.
class AssistantWebhookConfig {
  const AssistantWebhookConfig({
    this.enabled = false,
    this.url = '',
    this.secret = '',
    this.headerName = defaultHeaderName,
  });

  static const defaultHeaderName = 'Authorization';

  final bool enabled;
  final String url;
  final String secret;
  final String headerName;

  Uri? get uri {
    final parsed = Uri.tryParse(url.trim());
    if (parsed == null ||
        !(parsed.isScheme('https') || parsed.isScheme('http')) ||
        parsed.host.isEmpty) {
      return null;
    }
    return parsed;
  }

  bool get ready => enabled && uri != null;

  /// `Authorization` gets `Bearer <key>`; any other header gets the raw key.
  Map<String, String> get headers {
    final headers = <String, String>{
      'Content-Type': 'application/json; charset=utf-8',
    };
    final key = secret.trim();
    if (key.isEmpty) return headers;
    final name = headerName.trim().isEmpty
        ? defaultHeaderName
        : headerName.trim();
    if (name.toLowerCase() == 'authorization') {
      headers['Authorization'] = key.toLowerCase().startsWith('bearer ')
          ? key
          : 'Bearer $key';
    } else {
      headers[name] = key;
    }
    return headers;
  }
}

/// Formats a local time as ISO 8601 with an explicit UTC offset.
String iso8601WithOffset(DateTime value) {
  final local = value.toLocal();
  String two(int v) => v.toString().padLeft(2, '0');
  final offset = local.timeZoneOffset;
  final sign = offset.isNegative ? '-' : '+';
  final minutes = offset.inMinutes.abs();
  return '${local.year.toString().padLeft(4, '0')}-${two(local.month)}-'
      '${two(local.day)}T${two(local.hour)}:${two(local.minute)}:'
      '${two(local.second)}$sign${two(minutes ~/ 60)}:${two(minutes % 60)}';
}

/// Stable (cross-launch) text fingerprint used for de-duplication.
String transcriptFingerprint(String text) {
  var hash = 0xcbf29ce484222325;
  const prime = 0x100000001b3;
  for (final byte in utf8.encode(text)) {
    hash ^= byte;
    hash = (hash * prime) & 0xFFFFFFFFFFFFFFFF;
  }
  return '${text.length}:${hash.toRadixString(16)}';
}

class AssistantWebhookEntry {
  AssistantWebhookEntry({
    required this.recordingKey,
    required this.recordingId,
    required this.path,
    required this.title,
    required this.transcript,
    required this.recordedAt,
    this.language,
    this.durationSec,
    this.attempts = 0,
    DateTime? notBefore,
  }) : notBefore = notBefore ?? DateTime.now();

  final String recordingKey;
  final String recordingId;
  String path;
  final String title;
  final String transcript;
  final String recordedAt;
  final String? language;
  double? durationSec;
  int attempts;
  DateTime notBefore;

  Map<String, Object?> payload() => {
    'source': 'soundcore',
    'recordedAt': recordedAt,
    'durationSec': durationSec ?? 0,
    'title': title,
    'transcript': transcript,
    'language': language,
    'recordingId': recordingId,
  };

  Map<String, Object?> toJson() => {
    'recordingKey': recordingKey,
    'recordingId': recordingId,
    'path': path,
    'title': title,
    'transcript': transcript,
    'recordedAt': recordedAt,
    'language': language,
    'durationSec': durationSec,
    'attempts': attempts,
    'notBefore': notBefore.toIso8601String(),
  };

  static AssistantWebhookEntry? fromJson(Object? json) {
    if (json is! Map) return null;
    final key = json['recordingKey'];
    final transcript = json['transcript'];
    if (key is! String || transcript is! String || transcript.trim().isEmpty) {
      return null;
    }
    return AssistantWebhookEntry(
      recordingKey: key,
      recordingId: '${json['recordingId'] ?? key}',
      path: '${json['path'] ?? ''}',
      title: '${json['title'] ?? ''}',
      transcript: transcript,
      recordedAt: '${json['recordedAt'] ?? iso8601WithOffset(DateTime.now())}',
      language: json['language'] is String ? json['language'] as String : null,
      durationSec: (json['durationSec'] as num?)?.toDouble(),
      attempts: json['attempts'] is int ? json['attempts'] as int : 0,
      notBefore: DateTime.tryParse('${json['notBefore']}'),
    );
  }
}

typedef AssistantPathResolver = String? Function(String recordingKey);
typedef AssistantBusyCheck = bool Function(String recordingKey);
typedef AssistantDurationReader = Future<Duration?> Function(String path);

/// Sends finished transcripts to a webhook with a small persisted retry queue.
class AssistantWebhookService {
  AssistantWebhookService({
    required this.config,
    required this.isBusy,
    required this.resolvePath,
    required this.readDuration,
    required this.onChanged,
    this.inMemory = false,
    http.Client? client,
    this.settleDelay = const Duration(seconds: 6),
  }) : _client = client ?? http.Client();

  static const _fileName = 'assistant_webhook_queue.json';
  static const _maxQueue = 50;
  static const _maxSentHistory = 300;
  static const _timeout = Duration(seconds: 20);

  final AssistantWebhookConfig Function() config;
  final AssistantBusyCheck isBusy;
  final AssistantPathResolver resolvePath;
  final AssistantDurationReader readDuration;
  final VoidCallback onChanged;
  final bool inMemory;
  final Duration settleDelay;
  final http.Client _client;

  final List<AssistantWebhookEntry> _queue = [];

  /// recordingKey -> fingerprint of the last transcript delivered.
  final Map<String, String> _sent = {};
  int _sentCount = 0;
  bool _flushing = false;
  bool _flushAgain = false;
  bool _disposed = false;
  Timer? _timer;
  Future<void> _loaded = Future<void>.value();
  Future<void> _pendingSave = Future<void>.value();

  /// Delivered during this app session.
  int get sentCount => _sentCount;

  /// Entries that already failed at least once and wait for a retry.
  int get failedCount => _queue.where((entry) => entry.attempts > 0).length;
  int get pendingCount => _queue.length;
  String? lastError;

  Future<void> load() {
    if (inMemory) return _loaded;
    _loaded = () async {
      try {
        final file = await _file();
        if (!await file.exists()) return;
        final json = jsonDecode(await file.readAsString());
        if (json is! Map) return;
        final queue = json['queue'];
        if (queue is List) {
          for (final raw in queue) {
            final entry = AssistantWebhookEntry.fromJson(raw);
            if (entry != null) _queue.add(entry);
          }
        }
        final sent = json['sent'];
        if (sent is Map) {
          for (final item in sent.entries) {
            if (item.value is String) _sent['${item.key}'] = item.value;
          }
        }
      } catch (error) {
        debugPrint('[AssistantWebhook] load failed: $error');
      }
    }();
    return _loaded;
  }

  /// Queue (or replace) the transcript for a recording. Live updates of the
  /// same recording collapse into one send after [settleDelay] of quiet.
  Future<void> enqueue({
    required String recordingKey,
    required String recordingId,
    required String path,
    required String title,
    required String transcript,
    required DateTime recordedAt,
    String? language,
    bool force = false,
  }) async {
    await _loaded;
    if (_disposed) return;
    final text = transcript.trim();
    if (text.isEmpty || !config().enabled) return;
    final fingerprint = transcriptFingerprint(text);
    if (!force && _sent[recordingKey] == fingerprint) return;
    _queue.removeWhere((entry) => entry.recordingKey == recordingKey);
    _queue.add(
      AssistantWebhookEntry(
        recordingKey: recordingKey,
        recordingId: recordingId,
        path: path,
        title: title,
        transcript: text,
        recordedAt: iso8601WithOffset(recordedAt),
        language: language,
        notBefore: DateTime.now().add(settleDelay),
      ),
    );
    while (_queue.length > _maxQueue) {
      _queue.removeAt(0);
    }
    unawaited(_save());
    onChanged();
    _schedule(settleDelay);
  }

  void _schedule(Duration delay) {
    if (_disposed) return;
    _timer?.cancel();
    _timer = Timer(delay + const Duration(milliseconds: 200), () {
      unawaited(flush());
    });
  }

  /// Sends due entries. Stops at the first failure; failed entries stay queued
  /// for the next launch, settings change, or the next successful send.
  Future<void> flush() async {
    await _loaded;
    if (_disposed) return;
    if (_flushing) {
      _flushAgain = true;
      return;
    }
    final cfg = config();
    if (!cfg.ready || _queue.isEmpty) return;
    _flushing = true;
    var changed = false;
    Duration? nextCheck;
    final backgroundTask = await AssistantBackgroundTask.begin();
    try {
      // New transcripts first; earlier failures retry after one succeeds.
      final ordered = [
        ..._queue.where((entry) => entry.attempts == 0),
        ..._queue.where((entry) => entry.attempts > 0),
      ];
      for (final entry in ordered) {
        if (_disposed) return;
        final now = DateTime.now();
        if (entry.notBefore.isAfter(now)) {
          final wait = entry.notBefore.difference(now);
          if (nextCheck == null || wait < nextCheck) nextCheck = wait;
          continue;
        }
        if (isBusy(entry.recordingKey)) {
          nextCheck = settleDelay;
          continue;
        }
        final current = resolvePath(entry.recordingKey);
        if (current != null && current.isNotEmpty) entry.path = current;
        if (entry.durationSec == null && entry.path.isNotEmpty) {
          final duration = await readDuration(entry.path);
          if (duration != null) {
            entry.durationSec = duration.inMilliseconds / 1000.0;
          }
        }
        final status = await _post(cfg, entry.payload());
        changed = true;
        if (status != null && status >= 200 && status < 300) {
          _queue.remove(entry);
          _sent.remove(entry.recordingKey);
          _sent[entry.recordingKey] = transcriptFingerprint(entry.transcript);
          while (_sent.length > _maxSentHistory) {
            _sent.remove(_sent.keys.first);
          }
          _sentCount++;
          lastError = null;
        } else {
          entry.attempts++;
          lastError = status == null ? '网络错误' : 'HTTP $status';
          break;
        }
      }
    } finally {
      await AssistantBackgroundTask.end(backgroundTask);
      _flushing = false;
      if (changed) {
        unawaited(_save());
        if (!_disposed) onChanged();
      }
    }
    if (_flushAgain) {
      _flushAgain = false;
      unawaited(flush());
    } else if (nextCheck != null) {
      _schedule(nextCheck);
    }
  }

  /// Posts a sample payload with the given config; returns a status text.
  Future<String> sendTest(AssistantWebhookConfig cfg) async {
    if (cfg.uri == null) return '请填写有效的 Webhook URL（https://…）';
    final status = await _post(cfg, {
      'source': 'soundcore',
      'test': true,
      'recordedAt': iso8601WithOffset(DateTime.now()),
      'durationSec': 0,
      'title': '测试消息',
      'transcript': '这是一条来自安克录音豆的测试消息。',
      'language': 'zh',
      'recordingId': 'test',
    });
    if (status == null) return '发送失败：${lastTestError ?? '网络错误'}';
    final ok = status >= 200 && status < 300;
    if (ok) unawaited(flush());
    return ok ? '发送成功（HTTP $status）' : '发送失败（HTTP $status）';
  }

  String? lastTestError;

  Future<int?> _post(
    AssistantWebhookConfig cfg,
    Map<String, Object?> payload,
  ) async {
    final uri = cfg.uri;
    if (uri == null) return null;
    try {
      final response = await _client
          .post(
            uri,
            headers: cfg.headers,
            body: utf8.encode(jsonEncode(payload)),
          )
          .timeout(_timeout);
      lastTestError = null;
      return response.statusCode;
    } catch (error) {
      debugPrint('[AssistantWebhook] post failed: $error');
      lastTestError = error is TimeoutException ? '请求超时' : '网络错误';
      return null;
    }
  }

  static Future<File> _file() async {
    Directory base;
    try {
      base = await getApplicationDocumentsDirectory();
    } catch (_) {
      base = Directory.systemTemp;
    }
    final dir = Directory(p.join(base.path, 'AnkerRecorder'));
    if (!await dir.exists()) await dir.create(recursive: true);
    return File(p.join(dir.path, _fileName));
  }

  Future<void> _save() {
    if (inMemory) return Future<void>.value();
    final encoded = jsonEncode({
      'version': 1,
      'queue': _queue.map((entry) => entry.toJson()).toList(),
      'sent': Map<String, String>.from(_sent),
    });
    final operation = _pendingSave.then((_) async {
      try {
        final file = await _file();
        final temporary = File('${file.path}.tmp');
        await temporary.writeAsString(encoded, flush: true);
        await temporary.rename(file.path);
      } catch (error) {
        debugPrint('[AssistantWebhook] save failed: $error');
      }
    });
    _pendingSave = operation.catchError((Object _) {});
    return operation;
  }

  void dispose() {
    _disposed = true;
    _timer?.cancel();
    _client.close();
  }
}

/// Asks iOS for extra background execution time while a send is in flight.
class AssistantBackgroundTask {
  AssistantBackgroundTask._();

  static const _channel = MethodChannel('soundcore/background_task');

  static Future<int?> begin() async {
    if (kIsWeb || !Platform.isIOS) return null;
    try {
      return await _channel.invokeMethod<int>('begin');
    } catch (_) {
      return null;
    }
  }

  static Future<void> end(int? id) async {
    if (id == null) return;
    try {
      await _channel.invokeMethod<void>('end', id);
    } catch (_) {}
  }
}
