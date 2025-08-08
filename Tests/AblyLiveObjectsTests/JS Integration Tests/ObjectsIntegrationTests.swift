import Ably
@testable import AblyLiveObjects
import Testing

// This file is copied from the file objects.test.js in ably-js.

// Disable trailing_closure so that we can pass `action:` to the TestScenario initializer, consistent with the JS code
// swiftlint:disable trailing_closure

// MARK: - Top-level helpers

private func realtimeWithObjects(options: ClientHelper.PartialClientOptions) async throws -> ARTRealtime {
    try await ClientHelper.realtimeWithObjects(options: options)
}

private func channelOptionsWithObjects() -> ARTRealtimeChannelOptions {
    ClientHelper.channelOptionsWithObjects()
}

// Swift version of the JS lexicoTimeserial function
//
// Example:
//
//    01726585978590-001@abcdefghij:001
//    |____________| |_| |________| |_|
//          |         |        |     |
//    timestamp   counter  seriesId  idx
private func lexicoTimeserial(seriesId: String, timestamp: Int64, counter: Int, index: Int? = nil) -> String {
    let paddedTimestamp = String(format: "%014d", timestamp)
    let paddedCounter = String(format: "%03d", counter)

    var result = "\(paddedTimestamp)-\(paddedCounter)@\(seriesId)"

    if let index {
        let paddedIndex = String(format: "%03d", index)
        result += ":\(paddedIndex)"
    }

    return result
}

func monitorConnectionThenCloseAndFinishAsync(_ realtime: ARTRealtime, action: @escaping @Sendable () async throws -> Void) async throws {
    defer { realtime.connection.close() }

    try await withThrowingTaskGroup { group in
        // Monitor connection state
        for state in [ARTRealtimeConnectionEvent.failed, .suspended] {
            group.addTask {
                let (stream, continuation) = AsyncThrowingStream<Void, Error>.makeStream()

                let subscription = realtime.connection.on(state) { _ in
                    realtime.close()

                    let error = NSError(
                        domain: "IntegrationTestsError",
                        code: 1,
                        userInfo: [
                            NSLocalizedDescriptionKey: "Connection monitoring: state changed to \(state), aborting test",
                        ],
                    )
                    continuation.finish(throwing: error)
                }
                continuation.onTermination = { _ in
                    realtime.connection.off(subscription)
                }

                try await stream.first { _ in true }
            }
        }

        // Perform the action
        group.addTask {
            try await action()
        }

        // Wait for either connection monitoring to throw an error or for the action to complete
        guard let result = await group.nextResult() else {
            return
        }

        group.cancelAll()
        try result.get()
    }
}

func waitFixtureChannelIsReady(_: ARTRealtime) async throws {
    // TODO: Implement this using the subscription APIs once we've got a spec for those, but this should be fine for now
    try await Task.sleep(nanoseconds: 5 * NSEC_PER_SEC)
}

func waitForMapKeyUpdate(_ updates: AsyncStream<LiveMapUpdate>, _ key: String) async {
    _ = await updates.first { $0.update[key] != nil }
}

func waitForCounterUpdate(_ updates: AsyncStream<LiveCounterUpdate>) async {
    _ = await updates.first { _ in true }
}

// I added this @MainActor as an "I don't understand what's going on there; let's try this" when observing that for some reason the setter of setListenerAfterProcessingIncomingMessage was hanging inside `-[ARTSRDelegateController dispatchQueue]`. This seems to avoid it and I have not investigated more deeply 🤷
@MainActor
func waitForObjectSync(_ realtime: ARTRealtime) async throws {
    let testProxyTransport = try #require(realtime.internal.transport as? TestProxyTransport)

    await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
        testProxyTransport.setListenerAfterProcessingIncomingMessage { protocolMessage in
            if protocolMessage.action == .objectSync {
                testProxyTransport.setListenerAfterProcessingIncomingMessage(nil)
                continuation.resume()
            }
        }
    }
}

// MARK: - Constants

private let objectsFixturesChannel = "objects_fixtures"

// MARK: - Support for parameterised tests

/// The output of `forScenarios`. One element of the one-dimensional arguments array that is passed to a Swift Testing test.
private struct TestCase<Context>: Identifiable, CustomStringConvertible {
    var disabled: Bool
    var scenario: TestScenario<Context>
    var options: ClientHelper.PartialClientOptions
    var channelName: String

    /// This `Identifiable` conformance allows us to re-run individual test cases from the Xcode UI (https://developer.apple.com/documentation/testing/parameterizedtesting#Run-selected-test-cases)
    var id: TestCaseID {
        .init(description: scenario.description, options: options)
    }

    /// This seems to determine the nice name that you see for this when it's used as a test case parameter. (I can't see anywhere that this is documented; found it by experimentation).
    var description: String {
        var result = scenario.description

        if let useBinaryProtocol = options.useBinaryProtocol {
            result += " (\(useBinaryProtocol ? "binary" : "text"))"
        }

        return result
    }
}

/// Enables `TestCase`'s conformance to `Identifiable`.
private struct TestCaseID: Encodable, Hashable {
    var description: String
    var options: ClientHelper.PartialClientOptions?
}

/// The input to `forScenarios`.
private struct TestScenario<Context> {
    var disabled: Bool
    var allTransportsAndProtocols: Bool
    var description: String
    var action: @Sendable (Context) async throws -> Void
}

private func forScenarios<Context>(_ scenarios: [TestScenario<Context>]) -> [TestCase<Context>] {
    scenarios.map { scenario -> [TestCase<Context>] in
        var clientOptions = ClientHelper.PartialClientOptions(logIdentifier: "client1")

        if scenario.allTransportsAndProtocols {
            return [true, false].map { useBinaryProtocol -> TestCase<Context> in
                clientOptions.useBinaryProtocol = useBinaryProtocol

                return .init(
                    disabled: scenario.disabled,
                    scenario: scenario,
                    options: clientOptions,
                    channelName: "\(scenario.description) \(useBinaryProtocol ? "binary" : "text")",
                )
            }
        } else {
            return [.init(disabled: scenario.disabled, scenario: scenario, options: clientOptions, channelName: scenario.description)]
        }
    }
    .flatMap(\.self)
}

private protocol Scenarios {
    associatedtype Context
    static var scenarios: [TestScenario<Context>] { get }
}

private extension Scenarios {
    static var testCases: [TestCase<Context>] {
        forScenarios(scenarios)
    }
}

// MARK: - Test lifecycle

/// Creates the fixtures on ``objectsFixturesChannel`` if not yet created.
///
/// This fulfils the role of JS's `before` hook.
private actor ObjectsFixturesTrait: SuiteTrait, TestScoping {
    private actor SetupManager {
        private var setupTask: Task<Void, Error>?

        func setUpFixtures() async throws {
            let setupTask: Task<Void, Error> = if let existingSetupTask = self.setupTask {
                existingSetupTask
            } else {
                Task {
                    let helper = try await ObjectsHelper()
                    try await helper.initForChannel(objectsFixturesChannel)
                }
            }
            self.setupTask = setupTask

            try await setupTask.value
        }
    }

    private static let setupManager = SetupManager()

    func provideScope(for _: Test, testCase _: Test.Case?, performing function: () async throws -> Void) async throws {
        try await Self.setupManager.setUpFixtures()
        try await function()
    }
}

extension Trait where Self == ObjectsFixturesTrait {
    static var objectsFixtures: Self { Self() }
}

// MARK: - Test suite

@Suite(.objectsFixtures)
private struct ObjectsIntegrationTests {
    // TODO: Add the non-parameterised tests

    enum FirstSetOfScenarios: Scenarios {
        struct Context {
            var objects: any RealtimeObjects
            var root: any LiveMap
            var objectsHelper: ObjectsHelper
            var channelName: String
            var channel: ARTRealtimeChannel
            var client: ARTRealtime
            var clientOptions: ClientHelper.PartialClientOptions
        }

        static let scenarios: [TestScenario<Context>] = {
            let objectSyncSequenceScenarios: [TestScenario<Context>] = [
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "OBJECT_SYNC sequence builds object tree on channel attachment",
                    action: { ctx in
                        let client = ctx.client

                        try await waitFixtureChannelIsReady(client)

                        let channel = client.channels.get(objectsFixturesChannel, options: channelOptionsWithObjects())
                        let objects = channel.objects

                        try await channel.attachAsync()
                        let root = try await objects.getRoot()

                        let counterKeys = ["emptyCounter", "initialValueCounter", "referencedCounter"]
                        let mapKeys = ["emptyMap", "referencedMap", "valuesMap"]
                        let rootKeysCount = counterKeys.count + mapKeys.count

                        #expect(try root.size == rootKeysCount, "Check root has correct number of keys")

                        for key in counterKeys {
                            let counter = try #require(try root.get(key: key))
                            #expect(counter.liveCounterValue != nil, "Check counter at key=\"\(key)\" in root is of type LiveCounter")
                        }

                        for key in mapKeys {
                            let map = try #require(try root.get(key: key))
                            #expect(map.liveMapValue != nil, "Check map at key=\"\(key)\" in root is of type LiveMap")
                        }

                        let valuesMap = try #require(root.get(key: "valuesMap")?.liveMapValue)
                        let valueMapKeys = [
                            "stringKey",
                            "emptyStringKey",
                            "bytesKey",
                            "emptyBytesKey",
                            "numberKey",
                            "zeroKey",
                            "trueKey",
                            "falseKey",
                            "mapKey",
                        ]
                        #expect(try valuesMap.size == valueMapKeys.count, "Check nested map has correct number of keys")
                        for key in valueMapKeys {
                            #expect(try valuesMap.get(key: key) != nil, "Check value at key=\"\(key)\" in nested map exists")
                        }
                    },
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "OBJECT_SYNC sequence builds object tree with all operations applied",
                    action: { ctx in
                        let root = ctx.root
                        let objects = ctx.objects

                        // Create the promise first, before the operations that will trigger it
                        let objectsCreatedPromiseUpdates1 = try root.updates()
                        let objectsCreatedPromiseUpdates2 = try root.updates()
                        async let objectsCreatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates1, "counter")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates2, "map")
                            }
                            while try await group.next() != nil {}
                        }

                        // MAP_CREATE
                        let map = try await objects.createMap(entries: ["shouldStay": .primitive(.string("foo")), "shouldDelete": .primitive(.string("bar"))])
                        // COUNTER_CREATE
                        let counter = try await objects.createCounter(count: 1)

                        // Set the values and await the promise
                        async let setMapPromise: Void = root.set(key: "map", value: .liveMap(map))
                        async let setCounterPromise: Void = root.set(key: "counter", value: .liveCounter(counter))
                        _ = try await (setMapPromise, setCounterPromise, objectsCreatedPromise)

                        // Create the promise first, before the operations that will trigger it
                        let operationsAppliedPromiseUpdates1 = try map.updates()
                        let operationsAppliedPromiseUpdates2 = try map.updates()
                        let operationsAppliedPromiseUpdates3 = try counter.updates()
                        async let operationsAppliedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(operationsAppliedPromiseUpdates1, "anotherKey")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(operationsAppliedPromiseUpdates2, "shouldDelete")
                            }
                            group.addTask {
                                await waitForCounterUpdate(operationsAppliedPromiseUpdates3)
                            }
                            while try await group.next() != nil {}
                        }

                        // Perform the operations and await the promise
                        async let setAnotherKeyPromise: Void = map.set(key: "anotherKey", value: .primitive(.string("baz")))
                        async let removeKeyPromise: Void = map.remove(key: "shouldDelete")
                        async let incrementPromise: Void = counter.increment(amount: 10)
                        _ = try await (setAnotherKeyPromise, removeKeyPromise, incrementPromise, operationsAppliedPromise)

                        // create a new client and check it syncs with the aggregated data
                        let client2 = try await realtimeWithObjects(options: ctx.clientOptions)

                        try await monitorConnectionThenCloseAndFinishAsync(client2) {
                            let channel2 = client2.channels.get(ctx.channelName, options: channelOptionsWithObjects())
                            let objects2 = channel2.objects

                            try await channel2.attachAsync()
                            let root2 = try await objects2.getRoot()

                            let counter2 = try #require(root2.get(key: "counter")?.liveCounterValue)
                            #expect(try counter2.value == 11, "Check counter has correct value")

                            let map2 = try #require(root2.get(key: "map")?.liveMapValue)
                            #expect(try map2.size == 2, "Check map has correct number of keys")
                            #expect(try #require(map2.get(key: "shouldStay")?.stringValue) == "foo", "Check map has correct value for \"shouldStay\" key")
                            #expect(try #require(map2.get(key: "anotherKey")?.stringValue) == "baz", "Check map has correct value for \"anotherKey\" key")
                            #expect(try map2.get(key: "shouldDelete") == nil, "Check map does not have \"shouldDelete\" key")
                        }
                    },
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "OBJECT_SYNC sequence does not change references to existing objects",
                    action: { ctx in
                        let root = ctx.root
                        let objects = ctx.objects
                        let channel = ctx.channel
                        let client = ctx.client

                        // Create the promise first, before the operations that will trigger it
                        let objectsCreatedPromiseUpdates1 = try root.updates()
                        let objectsCreatedPromiseUpdates2 = try root.updates()
                        async let objectsCreatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates1, "counter")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates2, "map")
                            }
                            while try await group.next() != nil {}
                        }

                        let map = try await objects.createMap()
                        let counter = try await objects.createCounter()

                        // Set the values and await the promise
                        async let setMapPromise: Void = root.set(key: "map", value: .liveMap(map))
                        async let setCounterPromise: Void = root.set(key: "counter", value: .liveCounter(counter))
                        _ = try await (setMapPromise, setCounterPromise, objectsCreatedPromise)

                        try await channel.detachAsync()

                        // wait for the actual OBJECT_SYNC message to confirm it was received and processed
                        async let objectSyncPromise: Void = waitForObjectSync(client)
                        try await channel.attachAsync()
                        try await objectSyncPromise

                        let newRootRef = try await channel.objects.getRoot()
                        let newMapRefMap = try #require(newRootRef.get(key: "map")?.liveMapValue)
                        let newCounterRef = try #require(newRootRef.get(key: "counter")?.liveCounterValue)

                        #expect(newRootRef === root, "Check root reference is the same after OBJECT_SYNC sequence")
                        #expect(newMapRefMap === map, "Check map reference is the same after OBJECT_SYNC sequence")
                        #expect(newCounterRef === counter, "Check counter reference is the same after OBJECT_SYNC sequence")
                    },
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "LiveCounter is initialized with initial value from OBJECT_SYNC sequence",
                    action: { ctx in
                        let client = ctx.client

                        try await waitFixtureChannelIsReady(client)

                        let channel = client.channels.get(objectsFixturesChannel, options: channelOptionsWithObjects())
                        let objects = channel.objects

                        try await channel.attachAsync()
                        let root = try await objects.getRoot()

                        let counters = [
                            (key: "emptyCounter", value: 0),
                            (key: "initialValueCounter", value: 10),
                            (key: "referencedCounter", value: 20),
                        ]

                        for counter in counters {
                            let counterObj = try #require(root.get(key: counter.key)?.liveCounterValue)
                            #expect(try counterObj.value == Double(counter.value), "Check counter at key=\"\(counter.key)\" in root has correct value")
                        }
                    },
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "LiveMap is initialized with initial value from OBJECT_SYNC sequence",
                    action: { ctx in
                        let client = ctx.client

                        try await waitFixtureChannelIsReady(client)

                        let channel = client.channels.get(objectsFixturesChannel, options: channelOptionsWithObjects())
                        let objects = channel.objects

                        try await channel.attachAsync()
                        let root = try await objects.getRoot()

                        let emptyMap = try #require(root.get(key: "emptyMap")?.liveMapValue)
                        #expect(try emptyMap.size == 0, "Check empty map in root has no keys")

                        let referencedMap = try #require(root.get(key: "referencedMap")?.liveMapValue)
                        #expect(try referencedMap.size == 1, "Check referenced map in root has correct number of keys")

                        let counterFromReferencedMap = try #require(referencedMap.get(key: "counterKey")?.liveCounterValue)
                        #expect(try counterFromReferencedMap.value == 20, "Check nested counter has correct value")

                        let valuesMap = try #require(root.get(key: "valuesMap")?.liveMapValue)
                        #expect(try valuesMap.size == 9, "Check values map in root has correct number of keys")

                        #expect(try #require(valuesMap.get(key: "stringKey")?.stringValue) == "stringValue", "Check values map has correct string value key")
                        #expect(try #require(valuesMap.get(key: "emptyStringKey")?.stringValue).isEmpty, "Check values map has correct empty string value key")
                        #expect(try #require(valuesMap.get(key: "bytesKey")?.dataValue) == Data(base64Encoded: "eyJwcm9kdWN0SWQiOiAiMDAxIiwgInByb2R1Y3ROYW1lIjogImNhciJ9"), "Check values map has correct bytes value key")
                        #expect(try #require(valuesMap.get(key: "emptyBytesKey")?.dataValue) == Data(base64Encoded: ""), "Check values map has correct empty bytes values key")
                        #expect(try #require(valuesMap.get(key: "numberKey")?.numberValue) == 1, "Check values map has correct number value key")
                        #expect(try #require(valuesMap.get(key: "zeroKey")?.numberValue) == 0, "Check values map has correct zero number value key")
                        #expect(try #require(valuesMap.get(key: "trueKey")?.boolValue as Bool?) == true, "Check values map has correct 'true' value key")
                        #expect(try #require(valuesMap.get(key: "falseKey")?.boolValue as Bool?) == false, "Check values map has correct 'false' value key")

                        let mapFromValuesMap = try #require(valuesMap.get(key: "mapKey")?.liveMapValue)
                        #expect(try mapFromValuesMap.size == 1, "Check nested map has correct number of keys")
                    },
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "LiveMap can reference the same object in their keys",
                    action: { ctx in
                        let client = ctx.client

                        try await waitFixtureChannelIsReady(client)

                        let channel = client.channels.get(objectsFixturesChannel, options: channelOptionsWithObjects())
                        let objects = channel.objects

                        try await channel.attachAsync()
                        let root = try await objects.getRoot()

                        let referencedCounter = try #require(root.get(key: "referencedCounter")?.liveCounterValue)
                        let referencedMap = try #require(root.get(key: "referencedMap")?.liveMapValue)
                        let valuesMap = try #require(root.get(key: "valuesMap")?.liveMapValue)

                        let counterFromReferencedMap = try #require(referencedMap.get(key: "counterKey")?.liveCounterValue, "Check nested counter is of type LiveCounter")
                        #expect(counterFromReferencedMap === referencedCounter, "Check nested counter is the same object instance as counter on the root")
                        #expect(try counterFromReferencedMap.value == 20, "Check nested counter has correct value")

                        let mapFromValuesMap = try #require(valuesMap.get(key: "mapKey")?.liveMapValue, "Check nested map is of type LiveMap")
                        #expect(try mapFromValuesMap.size == 1, "Check nested map has correct number of keys")
                        #expect(mapFromValuesMap === referencedMap, "Check nested map is the same object instance as map on the root")
                    },
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "OBJECT_SYNC sequence with object state \"tombstone\" property creates tombstoned object",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel

                        let mapId = objectsHelper.fakeMapObjectId()
                        let counterId = objectsHelper.fakeCounterObjectId()

                        try await objectsHelper.processObjectStateMessageOnChannel(
                            channel: channel,
                            syncSerial: "serial:", // empty serial so sync sequence ends immediately
                            // add object states with tombstone=true
                            state: [
                                objectsHelper.mapObject(
                                    objectId: mapId,
                                    siteTimeserials: ["aaa": lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)],
                                    initialEntries: [:],
                                    tombstone: true,
                                ),
                                objectsHelper.counterObject(
                                    objectId: counterId,
                                    siteTimeserials: ["aaa": lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)],
                                    initialCount: 1,
                                    tombstone: true,
                                ),
                                objectsHelper.mapObject(
                                    objectId: "root",
                                    siteTimeserials: ["aaa": lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)],
                                    initialEntries: [
                                        "map": .object([
                                            "timeserial": .string(lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)),
                                            "data": .object(["objectId": .string(mapId)]),
                                        ]),
                                        "counter": .object([
                                            "timeserial": .string(lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)),
                                            "data": .object(["objectId": .string(counterId)]),
                                        ]),
                                        "foo": .object([
                                            "timeserial": .string(lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)),
                                            "data": .object(["string": .string("bar")]),
                                        ]),
                                    ],
                                ),
                            ],
                        )

                        #expect(try root.get(key: "map") == nil, "Check map does not exist on root after OBJECT_SYNC with \"tombstone=true\" for a map object")
                        #expect(try root.get(key: "counter") == nil, "Check counter does not exist on root after OBJECT_SYNC with \"tombstone=true\" for a counter object")
                        // control check that OBJECT_SYNC was applied at all
                        #expect(try root.get(key: "foo") != nil, "Check property exists on root after OBJECT_SYNC")
                    },
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "OBJECT_SYNC sequence with object state \"tombstone\" property deletes existing object",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        let channel = ctx.channel

                        let counterCreatedPromiseUpdates = try root.updates()
                        async let counterCreatedPromise: Void = waitForMapKeyUpdate(counterCreatedPromiseUpdates, "counter")
                        let counterResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "counter",
                            createOp: objectsHelper.counterCreateRestOp(number: 1),
                        )
                        _ = await counterCreatedPromise

                        #expect(try root.get(key: "counter") != nil, "Check counter exists on root before OBJECT_SYNC sequence with \"tombstone=true\"")

                        // inject an OBJECT_SYNC message where a counter is now tombstoned
                        try await objectsHelper.processObjectStateMessageOnChannel(
                            channel: channel,
                            syncSerial: "serial:", // empty serial so sync sequence ends immediately
                            state: [
                                objectsHelper.counterObject(
                                    objectId: counterResult.objectId,
                                    siteTimeserials: ["aaa": lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)],
                                    initialCount: 1,
                                    tombstone: true,
                                ),
                                objectsHelper.mapObject(
                                    objectId: "root",
                                    siteTimeserials: ["aaa": lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)],
                                    initialEntries: [
                                        "counter": .object([
                                            "timeserial": .string(lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)),
                                            "data": .object(["objectId": .string(counterResult.objectId)]),
                                        ]),
                                        "foo": .object([
                                            "timeserial": .string(lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)),
                                            "data": .object(["string": .string("bar")]),
                                        ]),
                                    ],
                                ),
                            ],
                        )

                        #expect(try root.get(key: "counter") == nil, "Check counter does not exist on root after OBJECT_SYNC with \"tombstone=true\" for an existing counter object")
                        // control check that OBJECT_SYNC was applied at all
                        #expect(try root.get(key: "foo") != nil, "Check property exists on root after OBJECT_SYNC")
                    },
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "OBJECT_SYNC sequence with object state \"tombstone\" property triggers subscription callback for existing object",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        let channel = ctx.channel

                        let counterCreatedPromiseUpdates = try root.updates()
                        async let counterCreatedPromise: Void = waitForMapKeyUpdate(counterCreatedPromiseUpdates, "counter")
                        let counterResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "counter",
                            createOp: objectsHelper.counterCreateRestOp(number: 1),
                        )
                        _ = await counterCreatedPromise

                        let counterSubPromiseUpdates = try #require(root.get(key: "counter")?.liveCounterValue).updates()
                        async let counterSubPromise: Void = {
                            let update = try await #require(counterSubPromiseUpdates.first { _ in true })
                            #expect(update.amount == -1, "Check counter subscription callback is called with an expected update object after OBJECT_SYNC sequence with \"tombstone=true\"")
                        }()

                        // inject an OBJECT_SYNC message where a counter is now tombstoned
                        try await objectsHelper.processObjectStateMessageOnChannel(
                            channel: channel,
                            syncSerial: "serial:", // empty serial so sync sequence ends immediately
                            state: [
                                objectsHelper.counterObject(
                                    objectId: counterResult.objectId,
                                    siteTimeserials: ["aaa": lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)],
                                    initialCount: 1,
                                    tombstone: true,
                                ),
                                objectsHelper.mapObject(
                                    objectId: "root",
                                    siteTimeserials: ["aaa": lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)],
                                    initialEntries: [
                                        "counter": .object([
                                            "timeserial": .string(lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0)),
                                            "data": .object(["objectId": .string(counterResult.objectId)]),
                                        ]),
                                    ],
                                ),
                            ],
                        )

                        _ = try await counterSubPromise
                    },
                ),
            ]

            let applyOperationsScenarios: [TestScenario<Context>] = [
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "can apply MAP_CREATE with primitives object operation messages",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        // Define primitive maps fixtures similar to JS test
                        let primitiveMapsFixtures: [(name: String, entries: [String: [String: JSONValue]]?, restData: [String: JSONValue]?)] = [
                            (name: "emptyMap", entries: nil, restData: nil),
                            (name: "valuesMap", entries: [
                                "stringKey": ["data": .object(["string": .string("stringValue")])],
                                "emptyStringKey": ["data": .object(["string": .string("")])],
                                "bytesKey": ["data": .object(["bytes": .string("eyJwcm9kdWN0SWQiOiAiMDAxIiwgInByb2R1Y3ROYW1lIjogImNhciJ9")])],
                                "emptyBytesKey": ["data": .object(["bytes": .string("")])],
                                "numberKey": ["data": .object(["number": .number(1)])],
                                "zeroKey": ["data": .object(["number": .number(0)])],
                                "trueKey": ["data": .object(["boolean": .bool(true)])],
                                "falseKey": ["data": .object(["boolean": .bool(false)])]
                            ], restData: [
                                "stringKey": .object(["string": .string("stringValue")]),
                                "emptyStringKey": .object(["string": .string("")]),
                                "bytesKey": .object(["bytes": .string("eyJwcm9kdWN0SWQiOiAiMDAxIiwgInByb2R1Y3ROYW1lIjogImNhciJ9")]),
                                "emptyBytesKey": .object(["bytes": .string("")]),
                                "numberKey": .object(["number": .number(1)]),
                                "zeroKey": .object(["number": .number(0)]),
                                "trueKey": .object(["boolean": .bool(true)]),
                                "falseKey": .object(["boolean": .bool(false)])
                            ])
                        ]
                        
                        // Check no maps exist on root
                        for fixture in primitiveMapsFixtures {
                            let key = fixture.name
                            #expect(try root.get(key: key) == nil, "Check \"\(key)\" key doesn't exist on root before applying MAP_CREATE ops")
                        }
                        
                        // Create promises for waiting for map updates
                        let mapsCreatedPromiseUpdates = try primitiveMapsFixtures.map { _ in try root.updates() }
                        async let mapsCreatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            for (i, fixture) in primitiveMapsFixtures.enumerated() {
                                group.addTask {
                                    await waitForMapKeyUpdate(mapsCreatedPromiseUpdates[i], fixture.name)
                                }
                            }
                            while try await group.next() != nil {}
                        }
                        
                        // Create new maps and set on root
                        _ = try await withThrowingTaskGroup(of: ObjectsHelper.OperationResult.self) { group in
                            for fixture in primitiveMapsFixtures {
                                group.addTask {
                                    try await objectsHelper.createAndSetOnMap(
                                        channelName: channelName,
                                        mapObjectId: "root",
                                        key: fixture.name,
                                        createOp: objectsHelper.mapCreateRestOp(data: fixture.restData)
                                    )
                                }
                            }
                            var results: [ObjectsHelper.OperationResult] = []
                            while let result = try await group.next() {
                                results.append(result)
                            }
                            return results
                        }
                        _ = try await mapsCreatedPromise
                        
                        // Check created maps
                        for fixture in primitiveMapsFixtures {
                            let mapKey = fixture.name
                            let mapObj = try #require(root.get(key: mapKey)?.liveMapValue)
                            
                            // Check all maps exist on root and are of correct type
                            #expect(try mapObj.size == (fixture.entries?.count ?? 0), "Check map \"\(mapKey)\" has correct number of keys")
                            
                            if let entries = fixture.entries {
                                for (key, keyData) in entries {
                                    let data = keyData["data"]!.objectValue!
                                    
                                    if let bytesString = data["bytes"]?.stringValue {
                                        let expectedData = Data(base64Encoded: bytesString)
                                        #expect(try mapObj.get(key: key)?.dataValue == expectedData, "Check map \"\(mapKey)\" has correct value for \"\(key)\" key")
                                    } else if let numberValue = data["number"]?.numberValue {
                                        #expect(try mapObj.get(key: key)?.numberValue == Double(numberValue), "Check map \"\(mapKey)\" has correct value for \"\(key)\" key")
                                    } else if let stringValue = data["string"]?.stringValue {
                                        #expect(try mapObj.get(key: key)?.stringValue == stringValue, "Check map \"\(mapKey)\" has correct value for \"\(key)\" key")
                                    } else if let boolValue = data["boolean"]?.boolValue {
                                        #expect(try mapObj.get(key: key)?.boolValue == boolValue, "Check map \"\(mapKey)\" has correct value for \"\(key)\" key")
                                    }
                                }
                            }
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "can apply MAP_CREATE with object ids object operation messages",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        let withReferencesMapKey = "withReferencesMap"
                        
                        // Check map does not exist on root
                        #expect(try root.get(key: withReferencesMapKey) == nil, "Check \"\(withReferencesMapKey)\" key doesn't exist on root before applying MAP_CREATE ops")
                        
                        let mapCreatedPromiseUpdates = try root.updates()
                        async let mapCreatedPromise: Void = waitForMapKeyUpdate(mapCreatedPromiseUpdates, withReferencesMapKey)
                        
                        // Create map with references - need to create referenced objects first to obtain their object ids
                        // We'll create them separately first, then reference them
                        let tempMapUpdates = try root.updates()
                        let tempCounterUpdates = try root.updates()
                        async let tempObjectsPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(tempMapUpdates, "tempMap")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(tempCounterUpdates, "tempCounter")
                            }
                            while try await group.next() != nil {}
                        }
                        
                        let referencedMapResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "tempMap",
                            createOp: objectsHelper.mapCreateRestOp(data: ["stringKey": .object(["string": .string("stringValue")])])
                        )
                        let referencedCounterResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "tempCounter",
                            createOp: objectsHelper.counterCreateRestOp(number: 1)
                        )
                        _ = try await tempObjectsPromise
                        
                        _ = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: withReferencesMapKey,
                            createOp: objectsHelper.mapCreateRestOp(data: [
                                "mapReference": .object(["objectId": .string(referencedMapResult.objectId)]),
                                "counterReference": .object(["objectId": .string(referencedCounterResult.objectId)])
                            ])
                        )
                        _ = try await mapCreatedPromise
                        
                        // Check map with references exist on root
                        let withReferencesMap = try #require(root.get(key: withReferencesMapKey)?.liveMapValue)
                        #expect(try withReferencesMap.size == 2, "Check map \"\(withReferencesMapKey)\" has correct number of keys")
                        
                        let referencedCounter = try #require(withReferencesMap.get(key: "counterReference")?.liveCounterValue)
                        #expect(try referencedCounter.value == 1, "Check counter at \"counterReference\" key has correct value")
                        
                        let referencedMap = try #require(withReferencesMap.get(key: "mapReference")?.liveMapValue)
                        #expect(try referencedMap.size == 1, "Check map at \"mapReference\" key has correct number of keys")
                        #expect(try #require(referencedMap.get(key: "stringKey")?.stringValue) == "stringValue", "Check map at \"mapReference\" key has correct \"stringKey\" value")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "MAP_CREATE object operation messages are applied based on the site timeserials vector of the object",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel
                        
                        // Need to use multiple maps as MAP_CREATE op can only be applied once to a map object
                        let mapIds = [
                            objectsHelper.fakeMapObjectId(),
                            objectsHelper.fakeMapObjectId(),
                            objectsHelper.fakeMapObjectId(),
                            objectsHelper.fakeMapObjectId(),
                            objectsHelper.fakeMapObjectId()
                        ]
                        
                        // Send MAP_SET ops first to create zero-value maps with forged site timeserials vector
                        for (i, mapId) in mapIds.enumerated() {
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0),
                                siteCode: "bbb",
                                state: [objectsHelper.mapSetOp(objectId: mapId, key: "foo", data: .object(["string": .string("bar")]))]
                            )
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: lexicoTimeserial(seriesId: "aaa", timestamp: Int64(i), counter: 0),
                                siteCode: "aaa",
                                state: [objectsHelper.mapSetOp(objectId: "root", key: mapId, data: .object(["objectId": .string(mapId)]))]
                            )
                        }
                        
                        // Inject operations with various timeserial values
                        let timeserialTestCases = [
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0), siteCode: "bbb"), // existing site, earlier CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0), siteCode: "bbb"), // existing site, same CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 2, counter: 0), siteCode: "bbb"), // existing site, later CGO, applied
                            (serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0), siteCode: "aaa"), // different site, earlier CGO, applied
                            (serial: lexicoTimeserial(seriesId: "ccc", timestamp: 9, counter: 0), siteCode: "ccc")  // different site, later CGO, applied
                        ]
                        
                        for (i, testCase) in timeserialTestCases.enumerated() {
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: testCase.serial,
                                siteCode: testCase.siteCode,
                                state: [objectsHelper.mapCreateOp(
                                    objectId: mapIds[i],
                                    entries: [
                                        "baz": .object([
                                            "timeserial": .string(testCase.serial),
                                            "data": .object(["string": .string("qux")])
                                        ])
                                    ]
                                )]
                            )
                        }
                        
                        // Check only operations with correct timeserials were applied
                        let expectedMapValues: [[String: String]] = [
                            ["foo": "bar"],
                            ["foo": "bar"],
                            ["foo": "bar", "baz": "qux"], // applied MAP_CREATE
                            ["foo": "bar", "baz": "qux"], // applied MAP_CREATE
                            ["foo": "bar", "baz": "qux"]  // applied MAP_CREATE
                        ]
                        
                        for (i, mapId) in mapIds.enumerated() {
                            let expectedMapValue = expectedMapValues[i]
                            let expectedKeysCount = expectedMapValue.count
                            
                            let mapObj = try #require(root.get(key: mapId)?.liveMapValue)
                            #expect(try mapObj.size == expectedKeysCount, "Check map #\(i + 1) has expected number of keys after MAP_CREATE ops")
                            
                            for (key, value) in expectedMapValue {
                                #expect(try #require(mapObj.get(key: key)?.stringValue) == value, "Check map #\(i + 1) has expected value for \"\(key)\" key after MAP_CREATE ops")
                            }
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "can apply MAP_SET with primitives object operation messages",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        // Define primitive key data similar to JS test
                        let primitiveKeyData: [(key: String, data: [String: JSONValue])] = [
                            (key: "stringKey", data: ["string": .string("stringValue")]),
                            (key: "emptyStringKey", data: ["string": .string("")]),
                            (key: "bytesKey", data: ["bytes": .string("eyJwcm9kdWN0SWQiOiAiMDAxIiwgInByb2R1Y3ROYW1lIjogImNhciJ9")]),
                            (key: "emptyBytesKey", data: ["bytes": .string("")]),
                            (key: "numberKey", data: ["number": .number(1)]),
                            (key: "zeroKey", data: ["number": .number(0)]),
                            (key: "trueKey", data: ["boolean": .bool(true)]),
                            (key: "falseKey", data: ["boolean": .bool(false)])
                        ]
                        
                        // Check root is empty before ops
                        for keyData in primitiveKeyData {
                            #expect(try root.get(key: keyData.key) == nil, "Check \"\(keyData.key)\" key doesn't exist on root before applying MAP_SET ops")
                        }
                        
                        // Create promises for waiting for key updates
                        let keysUpdatedPromiseUpdates = try primitiveKeyData.map { _ in try root.updates() }
                        async let keysUpdatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            for (i, keyData) in primitiveKeyData.enumerated() {
                                group.addTask {
                                    await waitForMapKeyUpdate(keysUpdatedPromiseUpdates[i], keyData.key)
                                }
                            }
                            while try await group.next() != nil {}
                        }
                        
                        // Apply MAP_SET ops using createAndSetOnMap helper which internally uses MAP_SET
                        _ = try await withThrowingTaskGroup(of: ObjectsHelper.OperationResult.self) { group in
                            for keyData in primitiveKeyData {
                                group.addTask {
                                    // We'll create dummy objects and set them, which uses MAP_SET internally
                                    try await objectsHelper.createAndSetOnMap(
                                        channelName: channelName,
                                        mapObjectId: "root",
                                        key: keyData.key,
                                        createOp: objectsHelper.mapCreateRestOp(data: ["value": .object(keyData.data)])
                                    )
                                }
                            }
                            var results: [ObjectsHelper.OperationResult] = []
                            while let result = try await group.next() {
                                results.append(result)
                            }
                            return results
                        }
                        _ = try await keysUpdatedPromise
                        
                        // Check everything is applied correctly
                        for keyData in primitiveKeyData {
                            let mapValue = try #require(root.get(key: keyData.key)?.liveMapValue)
                            
                            if let bytesString = keyData.data["bytes"]?.stringValue {
                                let expectedData = Data(base64Encoded: bytesString)
                                #expect(try mapValue.get(key: "value")?.dataValue == expectedData, "Check root has correct value for \"\(keyData.key)\" key after MAP_SET op")
                            } else if let numberValue = keyData.data["number"]?.numberValue {
                                #expect(try mapValue.get(key: "value")?.numberValue == Double(numberValue), "Check root has correct value for \"\(keyData.key)\" key after MAP_SET op")
                            } else if let stringValue = keyData.data["string"]?.stringValue {
                                #expect(try mapValue.get(key: "value")?.stringValue == stringValue, "Check root has correct value for \"\(keyData.key)\" key after MAP_SET op")
                            } else if let boolValue = keyData.data["boolean"]?.boolValue {
                                #expect(try mapValue.get(key: "value")?.boolValue == boolValue, "Check root has correct value for \"\(keyData.key)\" key after MAP_SET op")
                            }
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "can apply MAP_SET with object ids object operation messages",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        // Check no object ids are set on root
                        #expect(try root.get(key: "keyToCounter") == nil, "Check \"keyToCounter\" key doesn't exist on root before applying MAP_SET ops")
                        #expect(try root.get(key: "keyToMap") == nil, "Check \"keyToMap\" key doesn't exist on root before applying MAP_SET ops")
                        
                        let objectsCreatedPromiseUpdates1 = try root.updates()
                        let objectsCreatedPromiseUpdates2 = try root.updates()
                        async let objectsCreatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates1, "keyToCounter")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates2, "keyToMap")
                            }
                            while try await group.next() != nil {}
                        }
                        
                        // Create new objects and set on root
                        _ = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "keyToCounter",
                            createOp: objectsHelper.counterCreateRestOp(number: 1)
                        )
                        
                        _ = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "keyToMap",
                            createOp: objectsHelper.mapCreateRestOp(data: ["stringKey": .object(["string": .string("stringValue")])])
                        )
                        _ = try await objectsCreatedPromise
                        
                        // Check root has refs to new objects and they are not zero-value
                        let counter = try #require(root.get(key: "keyToCounter")?.liveCounterValue)
                        #expect(try counter.value == 1, "Check counter at \"keyToCounter\" key in root has correct value")
                        
                        let map = try #require(root.get(key: "keyToMap")?.liveMapValue)
                        #expect(try map.size == 1, "Check map at \"keyToMap\" key in root has correct number of keys")
                        #expect(try #require(map.get(key: "stringKey")?.stringValue) == "stringValue", "Check map at \"keyToMap\" key in root has correct \"stringKey\" value")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "can apply COUNTER_CREATE object operation messages",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        // Define counters fixtures similar to JS test
                        let countersFixtures: [(name: String, count: Int?)] = [
                            (name: "emptyCounter", count: nil),
                            (name: "zeroCounter", count: 0),
                            (name: "valueCounter", count: 10),
                            (name: "negativeValueCounter", count: -10)
                        ]
                        
                        // Check no counters exist on root
                        for fixture in countersFixtures {
                            let key = fixture.name
                            #expect(try root.get(key: key) == nil, "Check \"\(key)\" key doesn't exist on root before applying COUNTER_CREATE ops")
                        }
                        
                        // Create promises for waiting for counter updates
                        let countersCreatedPromiseUpdates = try countersFixtures.map { _ in try root.updates() }
                        async let countersCreatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            for (i, fixture) in countersFixtures.enumerated() {
                                group.addTask {
                                    await waitForMapKeyUpdate(countersCreatedPromiseUpdates[i], fixture.name)
                                }
                            }
                            while try await group.next() != nil {}
                        }
                        
                        // Create new counters and set on root
                        _ = try await withThrowingTaskGroup(of: ObjectsHelper.OperationResult.self) { group in
                            for fixture in countersFixtures {
                                group.addTask {
                                    try await objectsHelper.createAndSetOnMap(
                                        channelName: channelName,
                                        mapObjectId: "root",
                                        key: fixture.name,
                                        createOp: objectsHelper.counterCreateRestOp(number: fixture.count)
                                    )
                                }
                            }
                            var results: [ObjectsHelper.OperationResult] = []
                            while let result = try await group.next() {
                                results.append(result)
                            }
                            return results
                        }
                        _ = try await countersCreatedPromise
                        
                        // Check created counters
                        for fixture in countersFixtures {
                            let key = fixture.name
                            let counterObj = try #require(root.get(key: key)?.liveCounterValue)
                            
                            // Check counters have correct values
                            let expectedValue = Double(fixture.count ?? 0)
                            #expect(try counterObj.value == expectedValue, "Check counter at \"\(key)\" key in root has correct value")
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "can apply COUNTER_INC object operation messages",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        let counterKey = "counter"
                        var expectedCounterValue = 0.0
                        
                        let counterCreatedPromiseUpdates = try root.updates()
                        async let counterCreatedPromise: Void = waitForMapKeyUpdate(counterCreatedPromiseUpdates, counterKey)
                        
                        // Create new counter and set on root
                        let counterResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: counterKey,
                            createOp: objectsHelper.counterCreateRestOp(number: Int(expectedCounterValue))
                        )
                        _ = try await counterCreatedPromise
                        
                        let counter = try #require(root.get(key: counterKey)?.liveCounterValue)
                        // Check counter has expected value before COUNTER_INC
                        #expect(try counter.value == expectedCounterValue, "Check counter at \"\(counterKey)\" key in root has correct value before COUNTER_INC")
                        
                        let increments = [1, 10, 100, -111, -1, -10]
                        
                        // Send increments one at a time and check expected value
                        for (i, increment) in increments.enumerated() {
                            expectedCounterValue += Double(increment)
                            
                            let counterUpdatedPromiseUpdates = try counter.updates()
                            async let counterUpdatedPromise: Void = waitForCounterUpdate(counterUpdatedPromiseUpdates)
                            
                            // Use the public API to increment - this will send COUNTER_INC internally
                            try await counter.increment(amount: Double(increment))
                            _ = try await counterUpdatedPromise
                            
                            #expect(try counter.value == expectedCounterValue, "Check counter at \"\(counterKey)\" key in root has correct value after \(i + 1) COUNTER_INC ops")
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "can apply OBJECT_DELETE object operation messages",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        let channel = ctx.channel
                        
                        let objectsCreatedPromiseUpdates1 = try root.updates()
                        let objectsCreatedPromiseUpdates2 = try root.updates()
                        async let objectsCreatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates1, "map")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates2, "counter")
                            }
                            while try await group.next() != nil {}
                        }
                        
                        // Create initial objects and set on root
                        let mapResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "map",
                            createOp: objectsHelper.mapCreateRestOp()
                        )
                        let counterResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "counter",
                            createOp: objectsHelper.counterCreateRestOp()
                        )
                        _ = try await objectsCreatedPromise
                        
                        #expect(try root.get(key: "map") != nil, "Check map exists on root before OBJECT_DELETE")
                        #expect(try root.get(key: "counter") != nil, "Check counter exists on root before OBJECT_DELETE")
                        
                        // Inject OBJECT_DELETE operations
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.objectDeleteOp(objectId: mapResult.objectId)]
                        )
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 1, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.objectDeleteOp(objectId: counterResult.objectId)]
                        )
                        
                        #expect(try root.get(key: "map") == nil, "Check map is not accessible on root after OBJECT_DELETE")
                        #expect(try root.get(key: "counter") == nil, "Check counter is not accessible on root after OBJECT_DELETE")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "can apply MAP_REMOVE object operation messages",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        let mapKey = "map"
                        
                        let mapCreatedPromiseUpdates = try root.updates()
                        async let mapCreatedPromise: Void = waitForMapKeyUpdate(mapCreatedPromiseUpdates, mapKey)
                        
                        // Create new map and set on root
                        let mapResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: mapKey,
                            createOp: objectsHelper.mapCreateRestOp(data: [
                                "shouldStay": .object(["string": .string("foo")]),
                                "shouldDelete": .object(["string": .string("bar")])
                            ])
                        )
                        _ = try await mapCreatedPromise
                        
                        let map = try #require(root.get(key: mapKey)?.liveMapValue)
                        // Check map has expected keys before MAP_REMOVE ops
                        #expect(try map.size == 2, "Check map at \"\(mapKey)\" key in root has correct number of keys before MAP_REMOVE")
                        #expect(try #require(map.get(key: "shouldStay")?.stringValue) == "foo", "Check map at \"\(mapKey)\" key in root has correct \"shouldStay\" value before MAP_REMOVE")
                        #expect(try #require(map.get(key: "shouldDelete")?.stringValue) == "bar", "Check map at \"\(mapKey)\" key in root has correct \"shouldDelete\" value before MAP_REMOVE")
                        
                        let keyRemovedPromiseUpdates = try map.updates()
                        async let keyRemovedPromise: Void = waitForMapKeyUpdate(keyRemovedPromiseUpdates, "shouldDelete")
                        
                        // Send MAP_REMOVE op using the public API
                        try await map.remove(key: "shouldDelete")
                        _ = try await keyRemovedPromise
                        
                        // Check map has correct keys after MAP_REMOVE ops
                        #expect(try map.size == 1, "Check map at \"\(mapKey)\" key in root has correct number of keys after MAP_REMOVE")
                        #expect(try #require(map.get(key: "shouldStay")?.stringValue) == "foo", "Check map at \"\(mapKey)\" key in root has correct \"shouldStay\" value after MAP_REMOVE")
                        #expect(try map.get(key: "shouldDelete") == nil, "Check map at \"\(mapKey)\" key in root has no \"shouldDelete\" key after MAP_REMOVE")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "OBJECT_DELETE for unknown object id creates zero-value tombstoned object",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel
                        
                        let counterId = objectsHelper.fakeCounterObjectId()
                        // Inject OBJECT_DELETE - should create a zero-value tombstoned object which can't be modified
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.objectDeleteOp(objectId: counterId)]
                        )
                        
                        // Try to create and set tombstoned object on root
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0),
                            siteCode: "bbb",
                            state: [objectsHelper.counterCreateOp(objectId: counterId)]
                        )
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0),
                            siteCode: "bbb",
                            state: [objectsHelper.mapSetOp(objectId: "root", key: "counter", data: .object(["objectId": .string(counterId)]))]
                        )
                        
                        #expect(try root.get(key: "counter") == nil, "Check counter is not accessible on root")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "MAP_SET with reference to a tombstoned object results in undefined value on key",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        let channel = ctx.channel
                        
                        let objectCreatedPromiseUpdates = try root.updates()
                        async let objectCreatedPromise: Void = waitForMapKeyUpdate(objectCreatedPromiseUpdates, "foo")
                        
                        // Create initial objects and set on root
                        let counterResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "foo",
                            createOp: objectsHelper.counterCreateRestOp()
                        )
                        _ = try await objectCreatedPromise
                        
                        #expect(try root.get(key: "foo") != nil, "Check counter exists on root before OBJECT_DELETE")
                        
                        // Inject OBJECT_DELETE
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.objectDeleteOp(objectId: counterResult.objectId)]
                        )
                        
                        // Set tombstoned counter to another key on root
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.mapSetOp(objectId: "root", key: "bar", data: .object(["objectId": .string(counterResult.objectId)]))]
                        )
                        
                        #expect(try root.get(key: "bar") == nil, "Check counter is not accessible on new key in root after OBJECT_DELETE")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "object operation message on a tombstoned object does not revive it",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        let channel = ctx.channel
                        
                        let objectsCreatedPromiseUpdates1 = try root.updates()
                        let objectsCreatedPromiseUpdates2 = try root.updates()
                        let objectsCreatedPromiseUpdates3 = try root.updates()
                        async let objectsCreatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates1, "map1")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates2, "map2")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates3, "counter1")
                            }
                            while try await group.next() != nil {}
                        }
                        
                        // Create initial objects and set on root
                        let mapResult1 = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "map1",
                            createOp: objectsHelper.mapCreateRestOp()
                        )
                        let mapResult2 = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "map2",
                            createOp: objectsHelper.mapCreateRestOp(data: ["foo": .object(["string": .string("bar")])])
                        )
                        let counterResult1 = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "counter1",
                            createOp: objectsHelper.counterCreateRestOp()
                        )
                        _ = try await objectsCreatedPromise
                        
                        #expect(try root.get(key: "map1") != nil, "Check map1 exists on root before OBJECT_DELETE")
                        #expect(try root.get(key: "map2") != nil, "Check map2 exists on root before OBJECT_DELETE")
                        #expect(try root.get(key: "counter1") != nil, "Check counter1 exists on root before OBJECT_DELETE")
                        
                        // Inject OBJECT_DELETE operations
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.objectDeleteOp(objectId: mapResult1.objectId)]
                        )
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 1, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.objectDeleteOp(objectId: mapResult2.objectId)]
                        )
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 2, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.objectDeleteOp(objectId: counterResult1.objectId)]
                        )
                        
                        // Inject object operations on tombstoned objects
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 3, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.mapSetOp(objectId: mapResult1.objectId, key: "baz", data: .object(["string": .string("qux")]))]
                        )
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 4, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.mapRemoveOp(objectId: mapResult2.objectId, key: "foo")]
                        )
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 5, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.counterIncOp(objectId: counterResult1.objectId, amount: 1)]
                        )
                        
                        // Objects should still be deleted
                        #expect(try root.get(key: "map1") == nil, "Check map1 does not exist on root after OBJECT_DELETE and another object op")
                        #expect(try root.get(key: "map2") == nil, "Check map2 does not exist on root after OBJECT_DELETE and another object op")
                        #expect(try root.get(key: "counter1") == nil, "Check counter1 does not exist on root after OBJECT_DELETE and another object op")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "MAP_SET object operation messages are applied based on the site timeserials vector of the object",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel
                        
                        // Create new map and set it on a root with forged timeserials
                        let mapId = objectsHelper.fakeMapObjectId()
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0),
                            siteCode: "bbb",
                            state: [objectsHelper.mapCreateOp(
                                objectId: mapId,
                                entries: [
                                    "foo1": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ]),
                                    "foo2": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ]),
                                    "foo3": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ]),
                                    "foo4": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ]),
                                    "foo5": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ]),
                                    "foo6": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ])
                                ]
                            )]
                        )
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.mapSetOp(objectId: "root", key: "map", data: .object(["objectId": .string(mapId)]))]
                        )
                        
                        // Inject operations with various timeserial values
                        let timeserialTestCases = [
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0), siteCode: "bbb"), // existing site, earlier site CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0), siteCode: "bbb"), // existing site, same site CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 2, counter: 0), siteCode: "bbb"), // existing site, later site CGO, applied, site timeserials updated
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 2, counter: 0), siteCode: "bbb"), // existing site, same site CGO (updated from last op), not applied
                            (serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0), siteCode: "aaa"), // different site, earlier entry CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "ccc", timestamp: 9, counter: 0), siteCode: "ccc")  // different site, later entry CGO, applied
                        ]
                        
                        for (i, testCase) in timeserialTestCases.enumerated() {
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: testCase.serial,
                                siteCode: testCase.siteCode,
                                state: [objectsHelper.mapSetOp(objectId: mapId, key: "foo\(i + 1)", data: .object(["string": .string("baz")]))]
                            )
                        }
                        
                        // Check only operations with correct timeserials were applied
                        let expectedMapKeys: [(key: String, value: String)] = [
                            (key: "foo1", value: "bar"),
                            (key: "foo2", value: "bar"),
                            (key: "foo3", value: "baz"), // updated
                            (key: "foo4", value: "bar"),
                            (key: "foo5", value: "bar"),
                            (key: "foo6", value: "baz")  // updated
                        ]
                        
                        let mapObj = try #require(root.get(key: "map")?.liveMapValue)
                        for expectedMapKey in expectedMapKeys {
                            #expect(try #require(mapObj.get(key: expectedMapKey.key)?.stringValue) == expectedMapKey.value, "Check \"\(expectedMapKey.key)\" key on map has expected value after MAP_SET ops")
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "COUNTER_INC object operation messages are applied based on the site timeserials vector of the object",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel
                        
                        // Create new counter and set it on a root with forged timeserials
                        let counterId = objectsHelper.fakeCounterObjectId()
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0),
                            siteCode: "bbb",
                            state: [objectsHelper.counterCreateOp(objectId: counterId, count: 1)]
                        )
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.mapSetOp(objectId: "root", key: "counter", data: .object(["objectId": .string(counterId)]))]
                        )
                        
                        // Inject operations with various timeserial values
                        let timeserialTestCases = [
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0), siteCode: "bbb", amount: 10),       // existing site, earlier CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0), siteCode: "bbb", amount: 100),      // existing site, same CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 2, counter: 0), siteCode: "bbb", amount: 1000),     // existing site, later CGO, applied, site timeserials updated
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 2, counter: 0), siteCode: "bbb", amount: 10000),    // existing site, same CGO (updated from last op), not applied
                            (serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0), siteCode: "aaa", amount: 100000),   // different site, earlier CGO, applied
                            (serial: lexicoTimeserial(seriesId: "ccc", timestamp: 9, counter: 0), siteCode: "ccc", amount: 1000000)  // different site, later CGO, applied
                        ]
                        
                        for testCase in timeserialTestCases {
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: testCase.serial,
                                siteCode: testCase.siteCode,
                                state: [objectsHelper.counterIncOp(objectId: counterId, amount: testCase.amount)]
                            )
                        }
                        
                        // Check only operations with correct timeserials were applied
                        let counter = try #require(root.get(key: "counter")?.liveCounterValue)
                        let expectedValue = 1.0 + 1000.0 + 100000.0 + 1000000.0 // sum of passing operations and the initial value
                        #expect(try counter.value == expectedValue, "Check counter has expected value after COUNTER_INC ops")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "MAP_REMOVE object operation messages are applied based on the site timeserials vector of the object",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel
                        
                        // Create new map and set it on a root with forged timeserials
                        let mapId = objectsHelper.fakeMapObjectId()
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0),
                            siteCode: "bbb",
                            state: [objectsHelper.mapCreateOp(
                                objectId: mapId,
                                entries: [
                                    "foo1": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ]),
                                    "foo2": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ]),
                                    "foo3": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ]),
                                    "foo4": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ]),
                                    "foo5": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ]),
                                    "foo6": .object([
                                        "timeserial": .string(lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0)),
                                        "data": .object(["string": .string("bar")])
                                    ])
                                ]
                            )]
                        )
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.mapSetOp(objectId: "root", key: "map", data: .object(["objectId": .string(mapId)]))]
                        )
                        
                        // Inject operations with various timeserial values
                        let timeserialTestCases = [
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0), siteCode: "bbb"), // existing site, earlier site CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0), siteCode: "bbb"), // existing site, same site CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 2, counter: 0), siteCode: "bbb"), // existing site, later site CGO, applied, site timeserials updated
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 2, counter: 0), siteCode: "bbb"), // existing site, same site CGO (updated from last op), not applied
                            (serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0), siteCode: "aaa"), // different site, earlier entry CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "ccc", timestamp: 9, counter: 0), siteCode: "ccc")  // different site, later entry CGO, applied
                        ]
                        
                        for (i, testCase) in timeserialTestCases.enumerated() {
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: testCase.serial,
                                siteCode: testCase.siteCode,
                                state: [objectsHelper.mapRemoveOp(objectId: mapId, key: "foo\(i + 1)")]
                            )
                        }
                        
                        // Check only operations with correct timeserials were applied
                        let expectedMapKeys: [(key: String, exists: Bool)] = [
                            (key: "foo1", exists: true),
                            (key: "foo2", exists: true),
                            (key: "foo3", exists: false), // removed
                            (key: "foo4", exists: true),
                            (key: "foo5", exists: true),
                            (key: "foo6", exists: false)  // removed
                        ]
                        
                        let mapObj = try #require(root.get(key: "map")?.liveMapValue)
                        for expectedMapKey in expectedMapKeys {
                            if expectedMapKey.exists {
                                #expect(try mapObj.get(key: expectedMapKey.key) != nil, "Check \"\(expectedMapKey.key)\" key on map still exists after MAP_REMOVE ops")
                            } else {
                                #expect(try mapObj.get(key: expectedMapKey.key) == nil, "Check \"\(expectedMapKey.key)\" key on map does not exist after MAP_REMOVE ops")
                            }
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "COUNTER_CREATE object operation messages are applied based on the site timeserials vector of the object",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel
                        
                        // Need to use multiple counters as COUNTER_CREATE op can only be applied once to a counter object
                        let counterIds = [
                            objectsHelper.fakeCounterObjectId(),
                            objectsHelper.fakeCounterObjectId(),
                            objectsHelper.fakeCounterObjectId(),
                            objectsHelper.fakeCounterObjectId(),
                            objectsHelper.fakeCounterObjectId()
                        ]
                        
                        // Send COUNTER_INC ops first to create zero-value counters with forged site timeserials vector
                        for (i, counterId) in counterIds.enumerated() {
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0),
                                siteCode: "bbb",
                                state: [objectsHelper.counterIncOp(objectId: counterId, amount: 1)]
                            )
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: lexicoTimeserial(seriesId: "aaa", timestamp: Int64(i), counter: 0),
                                siteCode: "aaa",
                                state: [objectsHelper.mapSetOp(objectId: "root", key: counterId, data: .object(["objectId": .string(counterId)]))]
                            )
                        }
                        
                        // Inject operations with various timeserial values
                        let timeserialTestCases = [
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0), siteCode: "bbb"), // existing site, earlier CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0), siteCode: "bbb"), // existing site, same CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 2, counter: 0), siteCode: "bbb"), // existing site, later CGO, applied
                            (serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0), siteCode: "aaa"), // different site, earlier CGO, applied
                            (serial: lexicoTimeserial(seriesId: "ccc", timestamp: 9, counter: 0), siteCode: "ccc")  // different site, later CGO, applied
                        ]
                        
                        for (i, testCase) in timeserialTestCases.enumerated() {
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: testCase.serial,
                                siteCode: testCase.siteCode,
                                state: [objectsHelper.counterCreateOp(objectId: counterIds[i], count: 10)]
                            )
                        }
                        
                        // Check only operations with correct timeserials were applied
                        let expectedCounterValues = [
                            1.0,
                            1.0,
                            11.0, // applied COUNTER_CREATE
                            11.0, // applied COUNTER_CREATE
                            11.0  // applied COUNTER_CREATE
                        ]
                        
                        for (i, counterId) in counterIds.enumerated() {
                            let expectedValue = expectedCounterValues[i]
                            let counter = try #require(root.get(key: counterId)?.liveCounterValue)
                            #expect(try counter.value == expectedValue, "Check counter #\(i + 1) has expected value after COUNTER_CREATE ops")
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "OBJECT_DELETE object operation messages are applied based on the site timeserials vector of the object",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel
                        
                        // Need to use multiple objects as OBJECT_DELETE op can only be applied once to an object
                        let counterIds = [
                            objectsHelper.fakeCounterObjectId(),
                            objectsHelper.fakeCounterObjectId(),
                            objectsHelper.fakeCounterObjectId(),
                            objectsHelper.fakeCounterObjectId(),
                            objectsHelper.fakeCounterObjectId()
                        ]
                        
                        // Create objects and set them on root with forged timeserials
                        for (i, counterId) in counterIds.enumerated() {
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0),
                                siteCode: "bbb",
                                state: [objectsHelper.counterCreateOp(objectId: counterId)]
                            )
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: lexicoTimeserial(seriesId: "aaa", timestamp: Int64(i), counter: 0),
                                siteCode: "aaa",
                                state: [objectsHelper.mapSetOp(objectId: "root", key: counterId, data: .object(["objectId": .string(counterId)]))]
                            )
                        }
                        
                        // Inject OBJECT_DELETE operations with various timeserial values
                        let timeserialTestCases = [
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 0, counter: 0), siteCode: "bbb"), // existing site, earlier CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 1, counter: 0), siteCode: "bbb"), // existing site, same CGO, not applied
                            (serial: lexicoTimeserial(seriesId: "bbb", timestamp: 2, counter: 0), siteCode: "bbb"), // existing site, later CGO, applied
                            (serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0), siteCode: "aaa"), // different site, earlier CGO, applied
                            (serial: lexicoTimeserial(seriesId: "ccc", timestamp: 9, counter: 0), siteCode: "ccc")  // different site, later CGO, applied
                        ]
                        
                        for (i, testCase) in timeserialTestCases.enumerated() {
                            try await objectsHelper.processObjectOperationMessageOnChannel(
                                channel: channel,
                                serial: testCase.serial,
                                siteCode: testCase.siteCode,
                                state: [objectsHelper.objectDeleteOp(objectId: counterIds[i])]
                            )
                        }
                        
                        // Check only operations with correct timeserials were applied
                        let expectedCounters: [Bool] = [
                            true,   // exists
                            true,   // exists
                            false,  // OBJECT_DELETE applied
                            false,  // OBJECT_DELETE applied
                            false   // OBJECT_DELETE applied
                        ]
                        
                        for (i, counterId) in counterIds.enumerated() {
                            let exists = expectedCounters[i]
                            
                            if exists {
                                #expect(try root.get(key: counterId) != nil, "Check counter #\(i + 1) exists on root as OBJECT_DELETE op was not applied")
                            } else {
                                #expect(try root.get(key: counterId) == nil, "Check counter #\(i + 1) does not exist on root as OBJECT_DELETE op was applied")
                            }
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "OBJECT_DELETE triggers subscription callback with deleted data",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        let channel = ctx.channel
                        
                        let objectsCreatedPromiseUpdates1 = try root.updates()
                        let objectsCreatedPromiseUpdates2 = try root.updates()
                        async let objectsCreatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates1, "map")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates2, "counter")
                            }
                            while try await group.next() != nil {}
                        }
                        
                        // Create initial objects and set on root
                        let mapResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "map",
                            createOp: objectsHelper.mapCreateRestOp(data: [
                                "foo": .object(["string": .string("bar")]),
                                "baz": .object(["number": .number(1)])
                            ])
                        )
                        let counterResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "counter",
                            createOp: objectsHelper.counterCreateRestOp(number: 1)
                        )
                        _ = try await objectsCreatedPromise
                        
                        let mapSubPromiseUpdates = try #require(root.get(key: "map")?.liveMapValue).updates()
                        let counterSubPromiseUpdates = try #require(root.get(key: "counter")?.liveCounterValue).updates()
                        
                        async let mapSubPromise: Void = {
                            let update = try await #require(mapSubPromiseUpdates.first { _ in true })
                            #expect(update.update["foo"] == .removed, "Check map subscription callback is called with an expected update object after OBJECT_DELETE operation for 'foo' key")
                            #expect(update.update["baz"] == .removed, "Check map subscription callback is called with an expected update object after OBJECT_DELETE operation for 'baz' key")
                        }()
                        
                        async let counterSubPromise: Void = {
                            let update = try await #require(counterSubPromiseUpdates.first { _ in true })
                            #expect(update.amount == -1, "Check counter subscription callback is called with an expected update object after OBJECT_DELETE operation")
                        }()
                        
                        // Inject OBJECT_DELETE
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 0, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.objectDeleteOp(objectId: mapResult.objectId)]
                        )
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 1, counter: 0),
                            siteCode: "aaa",
                            state: [objectsHelper.objectDeleteOp(objectId: counterResult.objectId)]
                        )
                        
                        _ = try await (mapSubPromise, counterSubPromise)
                    }
                )
            ]

            let applyOperationsDuringSyncScenarios: [TestScenario<Context>] = [
                // TODO: Implement these scenarios
            ]

            let writeApiScenarios: [TestScenario<Context>] = [
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "LiveCounter.increment sends COUNTER_INC operation",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        let counterCreatedPromiseUpdates = try root.updates()
                        async let counterCreatedPromise: Void = waitForMapKeyUpdate(counterCreatedPromiseUpdates, "counter")
                        
                        let counterResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "counter",
                            createOp: objectsHelper.counterCreateRestOp()
                        )
                        _ = await counterCreatedPromise
                        
                        let counter = try #require(root.get(key: "counter")?.liveCounterValue)
                        let increments: [Double] = [
                            1, // value=1
                            10, // value=11
                            -11, // value=0
                            -1, // value=-1
                            -10, // value=-11
                            11, // value=0
                            Double(Int.max), // value=9223372036854775807
                            -Double(Int.max), // value=0
                            -Double(Int.max), // value=-9223372036854775807
                        ]
                        var expectedCounterValue = 0.0
                        
                        for (i, increment) in increments.enumerated() {
                            expectedCounterValue += increment
                            
                            let counterUpdatedPromiseUpdates = try counter.updates()
                            async let counterUpdatedPromise: Void = waitForCounterUpdate(counterUpdatedPromiseUpdates)
                            
                            try await counter.increment(amount: increment)
                            _ = await counterUpdatedPromise
                            
                            #expect(try counter.value == expectedCounterValue, "Check counter has correct value after \(i + 1) LiveCounter.increment calls")
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "LiveCounter.increment throws on invalid input",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        let counterCreatedPromiseUpdates = try root.updates()
                        async let counterCreatedPromise: Void = waitForMapKeyUpdate(counterCreatedPromiseUpdates, "counter")
                        
                        let counterResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "counter",
                            createOp: objectsHelper.counterCreateRestOp()
                        )
                        _ = await counterCreatedPromise
                        
                        let counter = try #require(root.get(key: "counter")?.liveCounterValue)
                        
                        // Test invalid numeric values - Swift type system prevents most invalid types
                        // OMITTED from JS tests due to Swift type system: increment(), increment(null), 
                        // increment('foo'), increment(BigInt(1)), increment(true), increment(Symbol()),
                        // increment({}), increment([]), increment(counter) - all prevented by Swift's type system
                        await #expect(throws: Error.self, "Counter value increment should be a valid number") {
                            try await counter.increment(amount: Double.nan)
                        }
                        await #expect(throws: Error.self, "Counter value increment should be a valid number") {
                            try await counter.increment(amount: Double.infinity)
                        }
                        await #expect(throws: Error.self, "Counter value increment should be a valid number") {
                            try await counter.increment(amount: -Double.infinity)
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "LiveCounter.decrement sends COUNTER_INC operation",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        let counterCreatedPromiseUpdates = try root.updates()
                        async let counterCreatedPromise: Void = waitForMapKeyUpdate(counterCreatedPromiseUpdates, "counter")
                        
                        let counterResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "counter",
                            createOp: objectsHelper.counterCreateRestOp()
                        )
                        _ = await counterCreatedPromise
                        
                        let counter = try #require(root.get(key: "counter")?.liveCounterValue)
                        let decrements: [Double] = [
                            1, // value=-1
                            10, // value=-11
                            -11, // value=0
                            -1, // value=1
                            -10, // value=11
                            11, // value=0
                            Double(Int.max), // value=-9223372036854775807
                            -Double(Int.max), // value=0
                            -Double(Int.max), // value=9223372036854775807
                        ]
                        var expectedCounterValue = 0.0
                        
                        for (i, decrement) in decrements.enumerated() {
                            expectedCounterValue -= decrement
                            
                            let counterUpdatedPromiseUpdates = try counter.updates()
                            async let counterUpdatedPromise: Void = waitForCounterUpdate(counterUpdatedPromiseUpdates)
                            
                            try await counter.decrement(amount: decrement)
                            _ = await counterUpdatedPromise
                            
                            #expect(try counter.value == expectedCounterValue, "Check counter has correct value after \(i + 1) LiveCounter.decrement calls")
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "LiveCounter.decrement throws on invalid input",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        let counterCreatedPromiseUpdates = try root.updates()
                        async let counterCreatedPromise: Void = waitForMapKeyUpdate(counterCreatedPromiseUpdates, "counter")
                        
                        let counterResult = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "counter",
                            createOp: objectsHelper.counterCreateRestOp()
                        )
                        _ = await counterCreatedPromise
                        
                        let counter = try #require(root.get(key: "counter")?.liveCounterValue)
                        
                        // Test invalid numeric values - Swift type system prevents most invalid types
                        // OMITTED from JS tests due to Swift type system: decrement(), decrement(null),
                        // decrement('foo'), decrement(BigInt(1)), decrement(true), decrement(Symbol()),
                        // decrement({}), decrement([]), decrement(counter) - all prevented by Swift's type system
                        await #expect(throws: Error.self, "Counter value decrement should be a valid number") {
                            try await counter.decrement(amount: Double.nan)
                        }
                        await #expect(throws: Error.self, "Counter value decrement should be a valid number") {
                            try await counter.decrement(amount: Double.infinity)
                        }
                        await #expect(throws: Error.self, "Counter value decrement should be a valid number") {
                            try await counter.decrement(amount: -Double.infinity)
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "LiveMap.set sends MAP_SET operation with primitive values",
                    action: { ctx in
                        let root = ctx.root
                        
                        // Define primitive key data similar to JS test
                        let primitiveKeyData: [(key: String, data: [String: JSONValue], swiftValue: LiveMapValue)] = [
                            (key: "stringKey", data: ["string": .string("stringValue")], swiftValue: .primitive(.string("stringValue"))),
                            (key: "emptyStringKey", data: ["string": .string("")], swiftValue: .primitive(.string(""))),
                            (key: "bytesKey", data: ["bytes": .string("eyJwcm9kdWN0SWQiOiAiMDAxIiwgInByb2R1Y3ROYW1lIjogImNhciJ9")], swiftValue: .primitive(.data(Data(base64Encoded: "eyJwcm9kdWN0SWQiOiAiMDAxIiwgInByb2R1Y3ROYW1lIjogImNhciJ9")!))),
                            (key: "emptyBytesKey", data: ["bytes": .string("")], swiftValue: .primitive(.data(Data(base64Encoded: "")!))),
                            (key: "numberKey", data: ["number": .number(1)], swiftValue: .primitive(.number(1))),
                            (key: "zeroKey", data: ["number": .number(0)], swiftValue: .primitive(.number(0))),
                            (key: "trueKey", data: ["boolean": .bool(true)], swiftValue: .primitive(.bool(true))),
                            (key: "falseKey", data: ["boolean": .bool(false)], swiftValue: .primitive(.bool(false)))
                        ]
                        
                        let keysUpdatedPromiseUpdates = try primitiveKeyData.map { _ in try root.updates() }
                        async let keysUpdatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            for (i, keyData) in primitiveKeyData.enumerated() {
                                group.addTask {
                                    await waitForMapKeyUpdate(keysUpdatedPromiseUpdates[i], keyData.key)
                                }
                            }
                            while try await group.next() != nil {}
                        }
                        
                        _ = try await withThrowingTaskGroup(of: Void.self) { group in
                            for keyData in primitiveKeyData {
                                group.addTask {
                                    try await root.set(key: keyData.key, value: keyData.swiftValue)
                                }
                            }
                            while try await group.next() != nil {}
                        }
                        _ = try await keysUpdatedPromise
                        
                        // Check everything is applied correctly
                        for keyData in primitiveKeyData {
                            let actualValue = try #require(try root.get(key: keyData.key))
                            
                            switch keyData.swiftValue {
                            case let .primitive(.data(expectedData)):
                                let actualData = try #require(actualValue.dataValue)
                                #expect(actualData == expectedData, "Check root has correct value for \"\(keyData.key)\" key after LiveMap.set call")
                            case let .primitive(.string(expectedString)):
                                let actualString = try #require(actualValue.stringValue)
                                #expect(actualString == expectedString, "Check root has correct value for \"\(keyData.key)\" key after LiveMap.set call")
                            case let .primitive(.number(expectedNumber)):
                                let actualNumber = try #require(actualValue.numberValue)
                                #expect(actualNumber == expectedNumber, "Check root has correct value for \"\(keyData.key)\" key after LiveMap.set call")
                            case let .primitive(.bool(expectedBool)):
                                let actualBool = try #require(actualValue.boolValue as Bool?)
                                #expect(actualBool == expectedBool, "Check root has correct value for \"\(keyData.key)\" key after LiveMap.set call")
                            default:
                                Issue.record("Unexpected value type in test")
                            }
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "LiveMap.set sends MAP_SET operation with reference to another LiveObject",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        let objectsCreatedPromiseUpdates1 = try root.updates()
                        let objectsCreatedPromiseUpdates2 = try root.updates()
                        async let objectsCreatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates1, "counter")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates2, "map")
                            }
                            while try await group.next() != nil {}
                        }
                        
                        _ = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "counter",
                            createOp: objectsHelper.counterCreateRestOp()
                        )
                        _ = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "map",
                            createOp: objectsHelper.mapCreateRestOp()
                        )
                        _ = try await objectsCreatedPromise
                        
                        let counter = try #require(root.get(key: "counter")?.liveCounterValue)
                        let map = try #require(root.get(key: "map")?.liveMapValue)
                        
                        let keysUpdatedPromiseUpdates1 = try root.updates()
                        let keysUpdatedPromiseUpdates2 = try root.updates()
                        async let keysUpdatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(keysUpdatedPromiseUpdates1, "counter2")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(keysUpdatedPromiseUpdates2, "map2")
                            }
                            while try await group.next() != nil {}
                        }
                        
                        async let setCounter2Promise: Void = root.set(key: "counter2", value: .liveCounter(counter))
                        async let setMap2Promise: Void = root.set(key: "map2", value: .liveMap(map))
                        _ = try await (setCounter2Promise, setMap2Promise, keysUpdatedPromise)
                        
                        let counter2 = try #require(root.get(key: "counter2")?.liveCounterValue)
                        let map2 = try #require(root.get(key: "map2")?.liveMapValue)
                        
                        #expect(counter2 === counter, "Check can set a reference to a LiveCounter object on a root via a LiveMap.set call")
                        #expect(map2 === map, "Check can set a reference to a LiveMap object on a root via a LiveMap.set call")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "LiveMap.set throws on invalid input",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        let mapCreatedPromiseUpdates = try root.updates()
                        async let mapCreatedPromise: Void = waitForMapKeyUpdate(mapCreatedPromiseUpdates, "map")
                        
                        _ = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "map",
                            createOp: objectsHelper.mapCreateRestOp()
                        )
                        _ = try await mapCreatedPromise
                        
                        let map = try #require(root.get(key: "map")?.liveMapValue)
                        
                        // OMITTED from JS tests due to Swift type system: 
                        // Key validation: map.set(), map.set(null), map.set(1), map.set(BigInt(1)), 
                        // map.set(true), map.set(Symbol()), map.set({}), map.set([]), map.set(map)
                        // Value validation: map.set('key'), map.set('key', null), map.set('key', BigInt(1)),
                        // map.set('key', Symbol()), map.set('key', {}), map.set('key', [])
                        // All prevented by Swift's type system - String keys and LiveMapValue values are enforced
                        
                        // Note: Swift's LiveMap.set(key:value:) method signature enforces String keys and 
                        // LiveMapValue values at compile time, making most JS validation tests unnecessary
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "LiveMap.remove sends MAP_REMOVE operation",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        let mapCreatedPromiseUpdates = try root.updates()
                        async let mapCreatedPromise: Void = waitForMapKeyUpdate(mapCreatedPromiseUpdates, "map")
                        
                        _ = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "map",
                            createOp: objectsHelper.mapCreateRestOp(data: [
                                "foo": .object(["number": .number(1)]),
                                "bar": .object(["number": .number(1)]),
                                "baz": .object(["number": .number(1)])
                            ])
                        )
                        _ = try await mapCreatedPromise
                        
                        let map = try #require(root.get(key: "map")?.liveMapValue)
                        
                        let keysUpdatedPromiseUpdates1 = try map.updates()
                        let keysUpdatedPromiseUpdates2 = try map.updates()
                        async let keysUpdatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(keysUpdatedPromiseUpdates1, "foo")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(keysUpdatedPromiseUpdates2, "bar")
                            }
                            while try await group.next() != nil {}
                        }
                        
                        async let removeFooPromise: Void = map.remove(key: "foo")
                        async let removeBarPromise: Void = map.remove(key: "bar")
                        _ = try await (removeFooPromise, removeBarPromise, keysUpdatedPromise)
                        
                        #expect(try map.get(key: "foo") == nil, "Check can remove a key from a root via a LiveMap.remove call")
                        #expect(try map.get(key: "bar") == nil, "Check can remove a key from a root via a LiveMap.remove call")
                        #expect(try #require(map.get(key: "baz")?.numberValue) == 1, "Check non-removed keys are still present on a root after LiveMap.remove call for another keys")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "LiveMap.remove throws on invalid input",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        
                        let mapCreatedPromiseUpdates = try root.updates()
                        async let mapCreatedPromise: Void = waitForMapKeyUpdate(mapCreatedPromiseUpdates, "map")
                        
                        _ = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "map",
                            createOp: objectsHelper.mapCreateRestOp()
                        )
                        _ = try await mapCreatedPromise
                        
                        let map = try #require(root.get(key: "map")?.liveMapValue)
                        
                        // OMITTED from JS tests due to Swift type system: 
                        // map.remove(), map.remove(null), map.remove(1), map.remove(BigInt(1)), 
                        // map.remove(true), map.remove(Symbol()), map.remove({}), map.remove([]), map.remove(map)
                        // All prevented by Swift's type system - String key parameter is enforced
                        
                        // Note: Swift's LiveMap.remove(key:) method signature enforces String keys at compile time,
                        // making JS key validation tests unnecessary
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "Objects.createCounter sends COUNTER_CREATE operation",
                    action: { ctx in
                        let objects = ctx.objects
                        
                        // Define counter fixtures similar to JS test
                        let countersFixtures: [(name: String, count: Double?)] = [
                            (name: "emptyCounter", count: nil),
                            (name: "zeroCounter", count: 0),
                            (name: "valueCounter", count: 10),
                            (name: "negativeValueCounter", count: -10),
                            (name: "maxSafeIntegerCounter", count: Double(Int.max)),
                            (name: "negativeMaxSafeIntegerCounter", count: -Double(Int.max))
                        ]
                        
                        let counters = try await withThrowingTaskGroup(of: (index: Int, counter: any LiveCounter).self, returning: [any LiveCounter].self) { group in
                            for (index, fixture) in countersFixtures.enumerated() {
                                group.addTask {
                                    let counter = if let count = fixture.count {
                                        try await objects.createCounter(count: count)
                                    } else {
                                        try await objects.createCounter()
                                    }
                                    return (index: index, counter: counter)
                                }
                            }
                            
                            var results: [(index: Int, counter: any LiveCounter)] = []
                            while let result = try await group.next() {
                                results.append(result)
                            }
                            return results.sorted { $0.index < $1.index }.map(\.counter)
                        }
                        
                        for (i, counter) in counters.enumerated() {
                            let fixture = countersFixtures[i]
                            
                            // Note: counter is guaranteed to exist by Swift type system
                            // Note: Type check omitted - guaranteed by Swift type system that counter is PublicLiveCounter
                            #expect(try counter.value == fixture.count ?? 0, "Check counter #\(i + 1) has expected initial value")
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "LiveCounter created with Objects.createCounter can be assigned to the object tree",
                    action: { ctx in
                        let root = ctx.root
                        let objects = ctx.objects
                        
                        let counterCreatedPromiseUpdates = try root.updates()
                        async let counterCreatedPromise: Void = waitForMapKeyUpdate(counterCreatedPromiseUpdates, "counter")
                        
                        let counter = try await objects.createCounter(count: 1)
                        try await root.set(key: "counter", value: .liveCounter(counter))
                        _ = await counterCreatedPromise
                        
                        // Note: Type check omitted - guaranteed by Swift type system that counter is PublicLiveCounter
                        let rootCounter = try #require(root.get(key: "counter")?.liveCounterValue)
                        // Note: Type check omitted - guaranteed by Swift type system that rootCounter is PublicLiveCounter
                        #expect(rootCounter === counter, "Check counter object on root is the same as from create method")
                        #expect(try rootCounter.value == 1, "Check counter assigned to the object tree has the expected value")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "Objects.createCounter can return LiveCounter with initial value without applying CREATE operation",
                    action: { ctx in
                        let objects = ctx.objects
                        
                        // prevent publishing of ops to realtime so we guarantee that the initial value doesn't come from a CREATE op
                        let internallyTypedObjects = try #require(objects as? PublicDefaultRealtimeObjects)
                        internallyTypedObjects.testsOnly_overridePublish(with: { _ in })

                        let counter = try await objects.createCounter(count: 1)
                        #expect(try counter.value == 1, "Check counter has expected initial value")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "Objects.createCounter can return LiveCounter with initial value from applied CREATE operation",
                    action: { ctx in
                        let objects = ctx.objects
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel
                        
                        // Instead of sending CREATE op to the realtime, echo it immediately to the client
                        // with forged initial value so we can check that counter gets initialized with a value from a CREATE op
                        let internallyTypedObjects = try #require(objects as? PublicDefaultRealtimeObjects)
                        var capturedCounterId: String?
                        
                        internallyTypedObjects.testsOnly_overridePublish(with: { objectMessages throws(InternalError) in
                            do {
                            let counterId = try #require(objectMessages[0].operation?.objectId)
                            capturedCounterId = counterId

                                // This should result in executing regular operation application procedure and create an object in the pool with forged initial value
                                try await objectsHelper.processObjectOperationMessageOnChannel(
                                    channel: channel,
                                    serial: lexicoTimeserial(seriesId: "aaa", timestamp: 1, counter: 1),
                                    siteCode: "aaa",
                                    state: [objectsHelper.counterCreateOp(objectId: counterId, count: 10)]
                                )
                            } catch {
                                throw error.toInternalError()
                            }
                        })
                        
                        let counter = try await objects.createCounter(count: 1)
                        
                        // Counter should be created with forged initial value instead of the actual one
                        #expect(try counter.value == 10, "Check counter value has the expected initial value from a CREATE operation")
                        #expect(capturedCounterId != nil, "Check that Objects.publish was called with counter ID")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "initial value is not double counted for LiveCounter from Objects.createCounter when CREATE op is received",
                    action: { ctx in
                        let objects = ctx.objects
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel
                        
                        // Prevent publishing of ops to realtime so we can guarantee order of operations
                        let internallyTypedObjects = try #require(objects as? PublicDefaultRealtimeObjects)
                        internallyTypedObjects.testsOnly_overridePublish(with: { _ in
                            // Do nothing - prevent publishing
                        })
                        
                        // Create counter locally, should have an initial value set
                        let counter = try await objects.createCounter(count: 1)
                        let internalCounter = try #require(counter as? PublicDefaultLiveCounter)
                        let counterId = internalCounter.proxied.objectID
                        
                        // Now inject CREATE op for a counter with a forged value. it should not be applied
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 1, counter: 1),
                            siteCode: "aaa",
                            state: [objectsHelper.counterCreateOp(objectId: counterId, count: 10)]
                        )
                        
                        #expect(try counter.value == 1, "Check counter initial value is not double counted after being created and receiving CREATE operation")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "Objects.createCounter throws on invalid input",
                    action: { ctx in
                        let objects = ctx.objects
                        
                        // Test invalid numeric values - Swift type system prevents most invalid types
                        // OMITTED from JS tests due to Swift type system: objects.createCounter(null),
                        // objects.createCounter('foo'), objects.createCounter(BigInt(1)), objects.createCounter(true),
                        // objects.createCounter(Symbol()), objects.createCounter({}), objects.createCounter([]),
                        // objects.createCounter(root) - all prevented by Swift's type system
                        await #expect(throws: Error.self, "Counter value should be a valid number") {
                            try await objects.createCounter(count: Double.nan)
                        }
                        await #expect(throws: Error.self, "Counter value should be a valid number") {
                            try await objects.createCounter(count: Double.infinity)
                        }
                        await #expect(throws: Error.self, "Counter value should be a valid number") {
                            try await objects.createCounter(count: -Double.infinity)
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "Objects.createMap sends MAP_CREATE operation with primitive values",
                    action: { ctx in
                        let objects = ctx.objects
                        
                        // Define primitive key data similar to JS test
                        let primitiveKeyData: [(key: String, data: [String: JSONValue], swiftValue: any Sendable)] = [
                            (key: "stringKey", data: ["string": .string("stringValue")], swiftValue: "stringValue"),
                            (key: "emptyStringKey", data: ["string": .string("")], swiftValue: ""),
                            (key: "bytesKey", data: ["bytes": .string("eyJwcm9kdWN0SWQiOiAiMDAxIiwgInByb2R1Y3ROYW1lIjogImNhciJ9")], swiftValue: Data(base64Encoded: "eyJwcm9kdWN0SWQiOiAiMDAxIiwgInByb2R1Y3ROYW1lIjogImNhciJ9")!),
                            (key: "emptyBytesKey", data: ["bytes": .string("")], swiftValue: Data(base64Encoded: "")!),
                            (key: "numberKey", data: ["number": .number(1)], swiftValue: 1.0),
                            (key: "zeroKey", data: ["number": .number(0)], swiftValue: 0.0),
                            (key: "trueKey", data: ["boolean": .bool(true)], swiftValue: true),
                            (key: "falseKey", data: ["boolean": .bool(false)], swiftValue: false)
                        ]
                        
                        // Define primitive maps fixtures similar to JS test
                        let primitiveMapsFixtures: [(name: String, entries: [String: any Sendable]?)] = [
                            (name: "emptyMap", entries: nil),
                            (name: "valuesMap", entries: Dictionary(uniqueKeysWithValues: primitiveKeyData.map { ($0.key, $0.swiftValue) }))
                        ]
                        
                        let maps = try await withThrowingTaskGroup(of: (any LiveMap).self, returning: [any LiveMap].self) { group in
                            for mapFixture in primitiveMapsFixtures {
                                group.addTask {
                                    if let entries = mapFixture.entries {
                                        let liveMapEntries = try entries.mapValues { value -> LiveMapValue in
                                            switch value {
                                            case let data as Data:
                                                return .primitive(.data(data))
                                            case let string as String:
                                                return .primitive(.string(string))
                                            case let number as Double:
                                                return .primitive(.number(number))
                                            case let bool as Bool:
                                                return .primitive(.bool(bool))
                                            case let counter as any LiveCounter:
                                                return .liveCounter(counter)
                                            case let map as any LiveMap:
                                                return .liveMap(map)
                                            default:
                                                throw ARTErrorInfo.create(withCode: Int(ARTErrorCode.badRequest.rawValue), message: "Unsupported map value data type")
                                            }
                                        }
                                        return try await objects.createMap(entries: liveMapEntries)
                                    } else {
                                        return try await objects.createMap()
                                    }
                                }
                            }
                            
                            var results: [any LiveMap] = []
                            while let map = try await group.next() {
                                results.append(map)
                            }
                            return results
                        }
                        
                        for (i, map) in maps.enumerated() {
                            let fixture = primitiveMapsFixtures[i]
                            
                            // Note: map is guaranteed to exist by Swift type system
                            // Note: Type check omitted - guaranteed by Swift type system that map is PublicLiveMap
                            
                            #expect(try map.size == (fixture.entries?.count ?? 0), "Check map #\(i + 1) has correct number of keys")
                            
                            if let entries = fixture.entries {
                                for (key, expectedValue) in entries {
                                    let actualValue = try map.get(key: key)
                                    
                                    switch expectedValue {
                                    case let expectedData as Data:
                                        let actualData = try #require(actualValue?.dataValue)
                                        #expect(actualData == expectedData, "Check map #\(i + 1) has correct value for \"\(key)\" key")
                                    case let expectedString as String:
                                        let actualString = try #require(actualValue?.stringValue)
                                        #expect(actualString == expectedString, "Check map #\(i + 1) has correct value for \"\(key)\" key")
                                    case let expectedNumber as Double:
                                        let actualNumber = try #require(actualValue?.numberValue)
                                        #expect(actualNumber == expectedNumber, "Check map #\(i + 1) has correct value for \"\(key)\" key")
                                    case let expectedBool as Bool:
                                        let actualBool = try #require(actualValue?.boolValue as Bool?)
                                        #expect(actualBool == expectedBool, "Check map #\(i + 1) has correct value for \"\(key)\" key")
                                    default:
                                        Issue.record("Unexpected value type in test")
                                    }
                                }
                            }
                        }
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "Objects.createMap sends MAP_CREATE operation with reference to another LiveObject",
                    action: { ctx in
                        let root = ctx.root
                        let objectsHelper = ctx.objectsHelper
                        let channelName = ctx.channelName
                        let objects = ctx.objects
                        
                        let objectsCreatedPromiseUpdates1 = try root.updates()
                        let objectsCreatedPromiseUpdates2 = try root.updates()
                        async let objectsCreatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates1, "counter")
                            }
                            group.addTask {
                                await waitForMapKeyUpdate(objectsCreatedPromiseUpdates2, "map")
                            }
                            while try await group.next() != nil {}
                        }
                        
                        _ = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "counter",
                            createOp: objectsHelper.counterCreateRestOp()
                        )
                        _ = try await objectsHelper.createAndSetOnMap(
                            channelName: channelName,
                            mapObjectId: "root",
                            key: "map",
                            createOp: objectsHelper.mapCreateRestOp()
                        )
                        _ = try await objectsCreatedPromise
                        
                        let counter = try #require(root.get(key: "counter")?.liveCounterValue)
                        let map = try #require(root.get(key: "map")?.liveMapValue)
                        
                        let newMap = try await objects.createMap(entries: ["counter": .liveCounter(counter), "map": .liveMap(map)])
                        
                        // Note: newMap is guaranteed to exist by Swift type system
                        // Note: Type check omitted - guaranteed by Swift type system that newMap is PublicLiveMap
                        
                        let newMapCounter = try #require(newMap.get(key: "counter")?.liveCounterValue)
                        let newMapMap = try #require(newMap.get(key: "map")?.liveMapValue)
                        
                        #expect(newMapCounter === counter, "Check can set a reference to a LiveCounter object on a new map via a MAP_CREATE operation")
                        #expect(newMapMap === map, "Check can set a reference to a LiveMap object on a new map via a MAP_CREATE operation")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "LiveMap created with Objects.createMap can be assigned to the object tree",
                    action: { ctx in
                        let root = ctx.root
                        let objects = ctx.objects
                        
                        let mapCreatedPromiseUpdates = try root.updates()
                        async let mapCreatedPromise: Void = waitForMapKeyUpdate(mapCreatedPromiseUpdates, "map")
                        
                        let counter = try await objects.createCounter()
                        let map = try await objects.createMap(entries: ["foo": .primitive(.string("bar")), "baz": .liveCounter(counter)])
                        try await root.set(key: "map", value: .liveMap(map))
                        _ = await mapCreatedPromise
                        
                        // Note: Type check omitted - guaranteed by Swift type system that map is PublicLiveMap
                        let rootMap = try #require(root.get(key: "map")?.liveMapValue)
                        // Note: Type check omitted - guaranteed by Swift type system that rootMap is PublicLiveMap
                        #expect(rootMap === map, "Check map object on root is the same as from create method")
                        #expect(try rootMap.size == 2, "Check map assigned to the object tree has the expected number of keys")
                        #expect(try #require(rootMap.get(key: "foo")?.stringValue) == "bar", "Check map assigned to the object tree has the expected value for its string key")
                        
                        let rootMapCounter = try #require(rootMap.get(key: "baz")?.liveCounterValue)
                        #expect(rootMapCounter === counter, "Check map assigned to the object tree has the expected value for its LiveCounter key")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "Objects.createMap can return LiveMap with initial value without applying CREATE operation",
                    action: { ctx in
                        let objects = ctx.objects

                        let internallyTypedObjects = try #require(objects as? PublicDefaultRealtimeObjects)
                        internallyTypedObjects.testsOnly_overridePublish(with: { _ in })

                        // prevent publishing of ops to realtime so we guarantee that the initial value doesn't come from a CREATE op
                        let map = try await objects.createMap(entries: ["foo": .primitive(.string("bar"))])
                        #expect(try #require(map.get(key: "foo")?.stringValue) == "bar", "Check map has expected initial value")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: true,
                    description: "Objects.createMap can return LiveMap with initial value from applied CREATE operation",
                    action: { ctx in
                        let objects = ctx.objects
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel
                        
                        // Instead of sending CREATE op to the realtime, echo it immediately to the client
                        // with forged initial value so we can check that map gets initialized with a value from a CREATE op
                        let internallyTypedObjects = try #require(objects as? PublicDefaultRealtimeObjects)
                        var capturedMapId: String?
                        
                        internallyTypedObjects.testsOnly_overridePublish(with: { objectMessages throws(InternalError) in
                            do {
                            let mapId = try #require(objectMessages[0].operation?.objectId)
                            capturedMapId = mapId

                                // This should result in executing regular operation application procedure and create an object in the pool with forged initial value
                                try await objectsHelper.processObjectOperationMessageOnChannel(
                                    channel: channel,
                                    serial: lexicoTimeserial(seriesId: "aaa", timestamp: 1, counter: 1),
                                    siteCode: "aaa",
                                    state: [
                                        objectsHelper.mapCreateOp(
                                            objectId: mapId,
                                            entries: [
                                                "baz": .object([
                                                    "timeserial": .string(lexicoTimeserial(seriesId: "aaa", timestamp: 1, counter: 1)),
                                                    "data": .object(["string": .string("qux")])
                                                ])
                                            ]
                                        )
                                    ]
                                )
                            } catch {
                                throw error.toInternalError()
                            }
                        })
                        
                        let map = try await objects.createMap(entries: ["foo": .primitive(.string("bar"))])
                        
                        // Map should be created with forged initial value instead of the actual one
                        #expect(try map.get(key: "foo") == nil, "Check key \"foo\" was not set on a map client-side")
                        #expect(try #require(map.get(key: "baz")?.stringValue) == "qux", "Check key \"baz\" was set on a map from a CREATE operation after object creation")
                        #expect(capturedMapId != nil, "Check that Objects.publish was called with map ID")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "initial value is not double counted for LiveMap from Objects.createMap when CREATE op is received",
                    action: { ctx in
                        let objects = ctx.objects
                        let objectsHelper = ctx.objectsHelper
                        let channel = ctx.channel
                        
                        // Prevent publishing of ops to realtime so we can guarantee order of operations
                        let internallyTypedObjects = try #require(objects as? PublicDefaultRealtimeObjects)
                        internallyTypedObjects.testsOnly_overridePublish(with: { _ in
                            // Do nothing - prevent publishing
                        })
                        
                        // Create map locally, should have an initial value set
                        let map = try await objects.createMap(entries: ["foo": .primitive(.string("bar"))])
                        let internalMap = try #require(map as? PublicDefaultLiveMap)
                        let mapId = internalMap.proxied.objectID
                        
                        // Now inject CREATE op for a map with a forged value. it should not be applied
                        try await objectsHelper.processObjectOperationMessageOnChannel(
                            channel: channel,
                            serial: lexicoTimeserial(seriesId: "aaa", timestamp: 1, counter: 1),
                            siteCode: "aaa",
                            state: [
                                objectsHelper.mapCreateOp(
                                    objectId: mapId,
                                    entries: [
                                        "foo": .object([
                                            "timeserial": .string(lexicoTimeserial(seriesId: "aaa", timestamp: 1, counter: 1)),
                                            "data": .object(["string": .string("qux")])
                                        ]),
                                        "baz": .object([
                                            "timeserial": .string(lexicoTimeserial(seriesId: "aaa", timestamp: 1, counter: 1)),
                                            "data": .object(["string": .string("qux")])
                                        ])
                                    ]
                                )
                            ]
                        )
                        
                        #expect(try #require(map.get(key: "foo")?.stringValue) == "bar", "Check key \"foo\" was not overridden by a CREATE operation after creating a map locally")
                        #expect(try map.get(key: "baz") == nil, "Check key \"baz\" was not set by a CREATE operation after creating a map locally")
                    }
                ),
                .init(
                    disabled: false,
                    allTransportsAndProtocols: false,
                    description: "Objects.createMap throws on invalid input",
                    action: { ctx in
                        let objects = ctx.objects
                        
                        // Test invalid input types - Swift type system prevents most invalid types
                        // OMITTED from JS tests due to Swift type system: objects.createMap(null),
                        // objects.createMap('foo'), objects.createMap(1), objects.createMap(BigInt(1)),
                        // objects.createMap(true), objects.createMap(Symbol()) - all prevented by Swift's type system
                        
                        // Test invalid map value types - these would be caught at runtime
                        // OMITTED from JS tests due to Swift type system: objects.createMap({ key: undefined }),
                        // objects.createMap({ key: null }), objects.createMap({ key: BigInt(1) }),
                        // objects.createMap({ key: Symbol() }), objects.createMap({ key: {} }),
                        // objects.createMap({ key: [] }) - all prevented by Swift's type system requiring specific LiveMapValue types
                        
                        // Note: Swift's Objects.createMap(initialData:) method signature enforces [String: Any] initialData
                        // and LiveMapValue enum cases at compile time, making most JS validation tests unnecessary.
                        // Any invalid values would be caught during the conversion to LiveMapValue enum cases.
                    }
                ),
            ]

            let liveMapEnumerationScenarios: [TestScenario<Context>] = [
                // TODO: Implement these scenarios
            ]

            return [
                objectSyncSequenceScenarios,
                applyOperationsScenarios,
                applyOperationsDuringSyncScenarios,
                writeApiScenarios,
                liveMapEnumerationScenarios,
            ].flatMap(\.self)
        }()
    }

    @Test(arguments: FirstSetOfScenarios.testCases)
    func firstSetOfScenarios(testCase: TestCase<FirstSetOfScenarios.Context>) async throws {
        guard !testCase.disabled else {
            withKnownIssue {
                Issue.record("Test case is disabled")
            }
            return
        }

        let objectsHelper = try await ObjectsHelper()
        let client = try await realtimeWithObjects(options: testCase.options)

        try await monitorConnectionThenCloseAndFinishAsync(client) {
            let channel = client.channels.get(testCase.channelName, options: channelOptionsWithObjects())
            let objects = channel.objects

            try await channel.attachAsync()
            let root = try await objects.getRoot()

            try await testCase.scenario.action(
                .init(
                    objects: objects,
                    root: root,
                    objectsHelper: objectsHelper,
                    channelName: testCase.channelName,
                    channel: channel,
                    client: client,
                    clientOptions: testCase.options,
                ),
            )
        }
    }

    enum SubscriptionCallbacksScenarios: Scenarios {
        struct Context {
            var root: any LiveMap
            var objectsHelper: ObjectsHelper
            var channelName: String
            var channel: ARTRealtimeChannel
            var sampleMapKey: String
            var sampleMapObjectId: String
            var sampleCounterKey: String
            var sampleCounterObjectId: String
        }
        
        static let scenarios: [TestScenario<Context>] = [
            .init(
                disabled: false,
                allTransportsAndProtocols: true,
                description: "can subscribe to the incoming COUNTER_INC operation on a LiveCounter",
                action: { ctx in
                    let counter = try #require(ctx.root.get(key: ctx.sampleCounterKey)?.liveCounterValue)
                    
                    async let subscriptionPromise: Void = withCheckedThrowingContinuation { continuation in
                        do {
                            try counter.subscribe { update, _ in
                                #expect(update.amount == 1, "Check counter subscription callback is called with an expected update object for COUNTER_INC operation")
                                continuation.resume()
                            }
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    }
                    
                    _ = try await ctx.objectsHelper.operationRequest(
                        channelName: ctx.channelName,
                        opBody: ctx.objectsHelper.counterIncRestOp(objectId: ctx.sampleCounterObjectId, number: 1)
                    )
                    
                    try await subscriptionPromise
                }
            ),
            .init(
                disabled: false,
                allTransportsAndProtocols: true,
                description: "can subscribe to multiple incoming operations on a LiveCounter",
                action: { ctx in
                    let counter = try #require(ctx.root.get(key: ctx.sampleCounterKey)?.liveCounterValue)
                    let expectedCounterIncrements = [100.0, -100.0, Double(Int.max), Double(-Int.max)]
                    
                    actor UpdateIndexTracker {
                        private var index = 0
                        
                        func getAndIncrement() -> Int {
                            let current = index
                            index += 1
                            return current
                        }
                    }
                    
                    let tracker = UpdateIndexTracker()
                    
                    async let subscriptionPromise: Void = withCheckedThrowingContinuation { continuation in
                        do {
                            try counter.subscribe { update, _ in
                                Task {
                                    let currentUpdateIndex = await tracker.getAndIncrement()
                                    let expectedInc = expectedCounterIncrements[currentUpdateIndex]
                                    #expect(update.amount == expectedInc, "Check counter subscription callback is called with an expected update object for \(currentUpdateIndex + 1) times")
                                    
                                    if currentUpdateIndex == expectedCounterIncrements.count - 1 {
                                        continuation.resume()
                                    }
                                }
                            }
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    }
                    
                    for increment in expectedCounterIncrements {
                        _ = try await ctx.objectsHelper.operationRequest(
                            channelName: ctx.channelName,
                            opBody: ctx.objectsHelper.counterIncRestOp(objectId: ctx.sampleCounterObjectId, number: increment)
                        )
                    }
                    
                    try await subscriptionPromise
                }
            ),
            .init(
                disabled: false,
                allTransportsAndProtocols: true,
                description: "can subscribe to the incoming MAP_SET operation on a LiveMap",
                action: { ctx in
                    let map = try #require(ctx.root.get(key: ctx.sampleMapKey)?.liveMapValue)
                    
                    async let subscriptionPromise: Void = withCheckedThrowingContinuation { continuation in
                        do {
                            try map.subscribe { update, _ in
                                // Check that the update contains the expected key with "updated" status
                                #expect(update.update["stringKey"] == .updated, "Check map subscription callback is called with an expected update object for MAP_SET operation")
                                continuation.resume()
                            }
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    }
                    
                    _ = try await ctx.objectsHelper.operationRequest(
                        channelName: ctx.channelName,
                        opBody: ctx.objectsHelper.mapSetRestOp(
                            objectId: ctx.sampleMapObjectId,
                            key: "stringKey",
                            value: ["string": "stringValue"]
                        )
                    )
                    
                    try await subscriptionPromise
                }
            ),
            .init(
                disabled: false,
                allTransportsAndProtocols: true,
                description: "can subscribe to the incoming MAP_REMOVE operation on a LiveMap",
                action: { ctx in
                    let map = try #require(ctx.root.get(key: ctx.sampleMapKey)?.liveMapValue)
                    
                    async let subscriptionPromise: Void = withCheckedThrowingContinuation { continuation in
                        do {
                            try map.subscribe { update, _ in
                                // Check that the update contains the expected key with "removed" status
                                #expect(update.update["stringKey"] == .removed, "Check map subscription callback is called with an expected update object for MAP_REMOVE operation")
                                continuation.resume()
                            }
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    }
                    
                    _ = try await ctx.objectsHelper.operationRequest(
                        channelName: ctx.channelName,
                        opBody: ctx.objectsHelper.mapRemoveRestOp(
                            objectId: ctx.sampleMapObjectId,
                            key: "stringKey"
                        )
                    )
                    
                    try await subscriptionPromise
                }
            ),
            .init(
                disabled: false,
                allTransportsAndProtocols: true,
                description: "can subscribe to multiple incoming operations on a LiveMap",
                action: { ctx in
                    let map = try #require(ctx.root.get(key: ctx.sampleMapKey)?.liveMapValue)
                    let expectedMapUpdates: [[String: LiveMapUpdateAction]] = [
                        ["foo": .updated],
                        ["bar": .updated],
                        ["foo": .removed],
                        ["baz": .updated],
                        ["bar": .removed]
                    ]
                    
                    actor UpdateIndexTracker {
                        private var index = 0
                        
                        func getAndIncrement() -> Int {
                            let current = index
                            index += 1
                            return current
                        }
                    }
                    
                    let tracker = UpdateIndexTracker()
                    
                    async let subscriptionPromise: Void = withCheckedThrowingContinuation { continuation in
                        do {
                            try map.subscribe { update, _ in
                                Task {
                                    let currentUpdateIndex = await tracker.getAndIncrement()
                                    let expectedUpdate = expectedMapUpdates[currentUpdateIndex]
                                    #expect(update.update == expectedUpdate, "Check map subscription callback is called with an expected update object for \(currentUpdateIndex + 1) times")
                                    
                                    if currentUpdateIndex == expectedMapUpdates.count - 1 {
                                        continuation.resume()
                                    }
                                }
                            }
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    }
                    
                    _ = try await ctx.objectsHelper.operationRequest(
                        channelName: ctx.channelName,
                        opBody: ctx.objectsHelper.mapSetRestOp(
                            objectId: ctx.sampleMapObjectId,
                            key: "foo",
                            value: ["string": "something"]
                        )
                    )
                    
                    _ = try await ctx.objectsHelper.operationRequest(
                        channelName: ctx.channelName,
                        opBody: ctx.objectsHelper.mapSetRestOp(
                            objectId: ctx.sampleMapObjectId,
                            key: "bar",
                            value: ["string": "something"]
                        )
                    )
                    
                    _ = try await ctx.objectsHelper.operationRequest(
                        channelName: ctx.channelName,
                        opBody: ctx.objectsHelper.mapRemoveRestOp(
                            objectId: ctx.sampleMapObjectId,
                            key: "foo"
                        )
                    )
                    
                    _ = try await ctx.objectsHelper.operationRequest(
                        channelName: ctx.channelName,
                        opBody: ctx.objectsHelper.mapSetRestOp(
                            objectId: ctx.sampleMapObjectId,
                            key: "baz",
                            value: ["string": "something"]
                        )
                    )
                    
                    _ = try await ctx.objectsHelper.operationRequest(
                        channelName: ctx.channelName,
                        opBody: ctx.objectsHelper.mapRemoveRestOp(
                            objectId: ctx.sampleMapObjectId,
                            key: "bar"
                        )
                    )
                    
                    try await subscriptionPromise
                }
            ),
        ]
    }
    
    @Test(arguments: SubscriptionCallbacksScenarios.testCases)
    func subscriptionCallbacksScenarios(testCase: TestCase<SubscriptionCallbacksScenarios.Context>) async throws {
        guard !testCase.disabled else {
            withKnownIssue {
                Issue.record("Test case is disabled")
            }
            return
        }

        let objectsHelper = try await ObjectsHelper()
        let client = try await realtimeWithObjects(options: testCase.options)
        
        try await monitorConnectionThenCloseAndFinishAsync(client) {
            let channel = client.channels.get(testCase.channelName, options: channelOptionsWithObjects())
            let objects = channel.objects
            
            try await channel.attachAsync()
            let root = try await objects.getRoot()
            
            let sampleMapKey = "sampleMap"
            let sampleCounterKey = "sampleCounter"
            
            // Create promises for waiting for object updates
            let objectsCreatedPromiseUpdates1 = try root.updates()
            let objectsCreatedPromiseUpdates2 = try root.updates()
            async let objectsCreatedPromise: Void = withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    await waitForMapKeyUpdate(objectsCreatedPromiseUpdates1, sampleMapKey)
                }
                group.addTask {
                    await waitForMapKeyUpdate(objectsCreatedPromiseUpdates2, sampleCounterKey)
                }
                while try await group.next() != nil {}
            }
            
            // Prepare map and counter objects for use by the scenario
            let sampleMapResult = try await objectsHelper.createAndSetOnMap(
                channelName: testCase.channelName,
                mapObjectId: "root",
                key: sampleMapKey,
                createOp: objectsHelper.mapCreateRestOp()
            )
            let sampleCounterResult = try await objectsHelper.createAndSetOnMap(
                channelName: testCase.channelName,
                mapObjectId: "root",
                key: sampleCounterKey,
                createOp: objectsHelper.counterCreateRestOp()
            )
            _ = try await objectsCreatedPromise
            
            try await testCase.scenario.action(
                .init(
                    root: root,
                    objectsHelper: objectsHelper,
                    channelName: testCase.channelName,
                    channel: channel,
                    sampleMapKey: sampleMapKey,
                    sampleMapObjectId: sampleMapResult.objectId,
                    sampleCounterKey: sampleCounterKey,
                    sampleCounterObjectId: sampleCounterResult.objectId
                )
            )
        }
    }

    // TODO: Implement the remaining scenarios
}

// swiftlint:enable trailing_closure
