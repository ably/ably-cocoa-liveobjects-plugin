import Ably
import AblyPlugin
@testable import AblyLiveObjects

final class MockCoreSDK: CoreSDK {
    /// Synchronizes access to all of this instance's mutable state.
    private let mutex = NSLock()

    private nonisolated(unsafe) var _channelState: AblyPlugin.RealtimeChannelState
    private nonisolated(unsafe) var _publishHandler: (([OutboundObjectMessage]) async throws(InternalError) -> Void)?

    init(channelState: AblyPlugin.RealtimeChannelState) {
        _channelState = channelState
    }

    func publish(objectMessages: [OutboundObjectMessage]) async throws(InternalError) {
        if let handler = _publishHandler {
            try await handler(objectMessages)
        } else {
            protocolRequirementNotImplemented()
        }
    }

    func testsOnly_overridePublish(with _: @escaping ([OutboundObjectMessage]) async throws(InternalError) -> Void) {
        protocolRequirementNotImplemented()
    }

    var channelState: AblyPlugin.RealtimeChannelState {
        get {
            mutex.withLock {
                _channelState
            }
        }
        set {
            mutex.withLock {
                _channelState = newValue
            }
        }
    }

    /// Sets a custom publish handler for testing
    func setPublishHandler(_ handler: @escaping ([OutboundObjectMessage]) async throws(InternalError) -> Void) {
        mutex.withLock {
            _publishHandler = handler
        }
    }
}
