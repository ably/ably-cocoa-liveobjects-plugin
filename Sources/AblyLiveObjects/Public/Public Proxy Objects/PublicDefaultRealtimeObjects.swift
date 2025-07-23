import Ably

/// The class that provides the public API for interacting with LiveObjects, via the ``ARTRealtimeChannel/objects`` property.
///
/// This is largely a wrapper around ``InternalDefaultRealtimeObjects``.
internal final class PublicDefaultRealtimeObjects: RealtimeObjects {
    private let proxied: InternalDefaultRealtimeObjects
    internal var testsOnly_proxied: InternalDefaultRealtimeObjects {
        proxied
    }

    // MARK: - Dependencies that hold a strong reference to `proxied`

    private let coreSDK: CoreSDK

    internal init(proxied: InternalDefaultRealtimeObjects, coreSDK: CoreSDK) {
        self.proxied = proxied
        self.coreSDK = coreSDK
    }

    // MARK: - `RealtimeObjects` protocol

    internal func getRoot() async throws(ARTErrorInfo) -> any LiveMap {
        let internalMap = try await proxied.getRoot(coreSDK: coreSDK)
        return PublicObjectsStore.shared.getOrCreateMap(
            proxying: internalMap,
            creationArgs: .init(
                coreSDK: coreSDK,
                delegate: proxied,
            ),
        )
    }

    internal func createMap(entries: [String: LiveMapValue]) async throws(ARTErrorInfo) -> any LiveMap {
        try await proxied.createMap(entries: entries)
    }

    internal func createMap() async throws(ARTErrorInfo) -> any LiveMap {
        try await proxied.createMap()
    }

    internal func createCounter(count: Double) async throws(ARTErrorInfo) -> any LiveCounter {
        try await proxied.createCounter(count: count)
    }

    internal func createCounter() async throws(ARTErrorInfo) -> any LiveCounter {
        try await proxied.createCounter()
    }

    internal func batch(callback: sending BatchCallback) async throws {
        try await proxied.batch(callback: callback)
    }

    internal func on(event: ObjectsEvent, callback: sending @escaping ObjectsEventCallback) -> any OnObjectsEventResponse {
        proxied.on(event: event, callback: callback)
    }

    internal func offAll() {
        proxied.offAll()
    }

    // MARK: - Test-only APIs

    // These are only used by our plumbingSmokeTest (the rest of our unit tests test the internal classes, not the public ones).

    internal var testsOnly_onChannelAttachedHasObjects: Bool? {
        proxied.testsOnly_onChannelAttachedHasObjects
    }

    internal var testsOnly_receivedObjectProtocolMessages: AsyncStream<[InboundObjectMessage]> {
        proxied.testsOnly_receivedObjectProtocolMessages
    }

    internal func testsOnly_sendObject(objectMessages: [OutboundObjectMessage]) async throws(InternalError) {
        try await proxied.testsOnly_sendObject(objectMessages: objectMessages, coreSDK: coreSDK)
    }

    internal var testsOnly_receivedObjectSyncProtocolMessages: AsyncStream<[InboundObjectMessage]> {
        proxied.testsOnly_receivedObjectSyncProtocolMessages
    }
}
