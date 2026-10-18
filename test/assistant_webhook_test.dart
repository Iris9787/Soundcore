import 'dart:convert';

import 'package:anker_recorder/assistant/assistant_webhook.dart';
import 'package:anker_recorder/state/app_settings_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';

void main() {
  test('Authorization header uses Bearer, custom headers send raw key', () {
    const auth = AssistantWebhookConfig(
      enabled: true,
      url: 'https://example.com/hook',
      secret: 'abc',
    );
    expect(auth.headers['Authorization'], 'Bearer abc');
    const custom = AssistantWebhookConfig(
      enabled: true,
      url: 'https://example.com/hook',
      secret: 'abc',
      headerName: 'X-Webhook-Secret',
    );
    expect(custom.headers['X-Webhook-Secret'], 'abc');
    expect(custom.headers.containsKey('Authorization'), isFalse);
    expect(const AssistantWebhookConfig(url: 'ftp://x').uri, isNull);
  });

  test('ISO 8601 timestamp carries an offset', () {
    final value = iso8601WithOffset(DateTime(2026, 10, 18, 21, 5, 9));
    expect(
      RegExp(r'^2026-10-18T21:05:09[+-]\d\d:\d\d$').hasMatch(value),
      isTrue,
    );
  });

  test('webhook settings round-trip', () {
    const original = AppSettings(
      assistantWebhookEnabled: true,
      assistantWebhookUrl: 'https://example.com/hook',
      assistantWebhookSecret: 's3cret',
      assistantWebhookHeader: 'X-Key',
    );
    final restored = AppSettings.fromJson(original.toJson());
    expect(restored.assistantWebhookEnabled, isTrue);
    expect(restored.assistantWebhookUrl, 'https://example.com/hook');
    expect(restored.assistantWebhookSecret, 's3cret');
    expect(restored.assistantWebhookHeader, 'X-Key');
    final defaults = AppSettings.fromJson(const {});
    expect(defaults.assistantWebhookEnabled, isFalse);
    expect(defaults.assistantWebhookHeader, 'Authorization');
  });

  test(
    'sends once, skips duplicates and empty text, retries failures',
    () async {
      final bodies = <Map<String, dynamic>>[];
      var fail = true;
      final client = MockClient((request) async {
        expect(request.headers['Authorization'], 'Bearer k');
        expect(request.headers['Content-Type'], contains('application/json'));
        bodies.add(jsonDecode(utf8.decode(request.bodyBytes)));
        return http.Response('', fail ? 500 : 200);
      });
      final service = AssistantWebhookService(
        config: () => const AssistantWebhookConfig(
          enabled: true,
          url: 'https://example.com/hook',
          secret: 'k',
        ),
        isBusy: (_) => false,
        resolvePath: (_) => null,
        readDuration: (_) async => const Duration(milliseconds: 12500),
        onChanged: () {},
        inMemory: true,
        client: client,
        settleDelay: Duration.zero,
      );
      addTearDown(service.dispose);

      Future<void> enqueue(String text, {bool force = false}) =>
          service.enqueue(
            recordingKey: 'file:1',
            recordingId: '1',
            path: '/tmp/1.wav',
            title: '会议',
            transcript: text,
            recordedAt: DateTime(2026, 10, 18, 21),
            language: 'zh-CN',
            force: force,
          );

      await enqueue('   ');
      expect(service.pendingCount, 0);

      await enqueue('请提醒我明天下午三点开会');
      await service.flush();
      expect(service.failedCount, 1);
      expect(service.sentCount, 0);

      fail = false;
      await service.flush();
      expect(service.failedCount, 0);
      expect(service.sentCount, 1);
      final last = bodies.last;
      expect(last['source'], 'soundcore');
      expect(last['transcript'], '请提醒我明天下午三点开会');
      expect(last['durationSec'], 12.5);
      expect(last['title'], '会议');
      expect(last['language'], 'zh-CN');
      expect(last['recordingId'], '1');

      await enqueue('请提醒我明天下午三点开会');
      expect(service.pendingCount, 0);
      await enqueue('请提醒我明天下午三点开会', force: true);
      await service.flush();
      expect(service.sentCount, 2);
    },
  );
}
