import Flutter
import UIKit

@main
@objc class AppDelegate: FlutterAppDelegate, FlutterImplicitEngineDelegate {
  override func application(
    _ application: UIApplication,
    didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?
  ) -> Bool {
    return super.application(application, didFinishLaunchingWithOptions: launchOptions)
  }

  func didInitializeImplicitFlutterEngine(_ engineBridge: FlutterImplicitEngineBridge) {
    GeneratedPluginRegistrant.register(with: engineBridge.pluginRegistry)
    AppleSpeechBridge.shared.configure(
      binaryMessenger: engineBridge.applicationRegistrar.messenger()
    )
    RecordingShortcutBridge.shared.configure(
      binaryMessenger: engineBridge.applicationRegistrar.messenger()
    )
    configureBackgroundTaskChannel(
      binaryMessenger: engineBridge.applicationRegistrar.messenger()
    )
  }

  /// Lets Dart finish a short network send after the app is backgrounded.
  private var backgroundTaskChannel: FlutterMethodChannel?

  private func configureBackgroundTaskChannel(binaryMessenger: FlutterBinaryMessenger) {
    let channel = FlutterMethodChannel(
      name: "soundcore/background_task",
      binaryMessenger: binaryMessenger
    )
    channel.setMethodCallHandler { call, result in
      switch call.method {
      case "begin":
        var taskId: UIBackgroundTaskIdentifier = .invalid
        taskId = UIApplication.shared.beginBackgroundTask(withName: "assistant-webhook") {
          if taskId != .invalid {
            UIApplication.shared.endBackgroundTask(taskId)
            taskId = .invalid
          }
        }
        result(taskId == .invalid ? nil : taskId.rawValue)
      case "end":
        if let raw = call.arguments as? Int {
          UIApplication.shared.endBackgroundTask(UIBackgroundTaskIdentifier(rawValue: raw))
        }
        result(nil)
      default:
        result(FlutterMethodNotImplemented)
      }
    }
    backgroundTaskChannel = channel
  }
}
