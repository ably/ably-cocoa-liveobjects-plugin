@testable import AblyLiveObjects
import Foundation
import Testing

final class ObjectCreationHelpersTests {
    @Test
    func creationOperationForLiveCounter_createsValidOperation() {
        let count = 42.0
        let timestamp = Date(timeIntervalSince1970: 1_234_567_890)

        let operation = ObjectCreationHelpers.creationOperationForLiveCounter(
            count: count,
            timestamp: timestamp,
        )

        // Verify the object ID format follows RTO14
        #expect(operation.objectID.hasPrefix("counter:"))
        #expect(operation.objectID.contains("@"))

        // Verify the ObjectMessage structure
        #expect(operation.objectMessage.operation?.action == .known(.counterCreate))
        #expect(operation.objectMessage.operation?.objectId == operation.objectID)
        #expect(operation.objectMessage.operation?.counter?.count == NSNumber(value: count))
        #expect(operation.objectMessage.operation?.nonce != nil)
        #expect(operation.objectMessage.operation?.initialValue != nil)
    }

    @Test
    func creationOperationForLiveMap_createsValidOperation() {
        let entries: [String: InternalLiveMapValue] = [
            "stringKey": .primitive(.string("stringValue")),
            "numberKey": .primitive(.number(123.45)),
            "boolKey": .primitive(.bool(true)),
            "dataKey": .primitive(.data(Data([1, 2, 3, 4]))),
        ]
        let timestamp = Date(timeIntervalSince1970: 1_234_567_890)

        let operation = ObjectCreationHelpers.creationOperationForLiveMap(
            entries: entries,
            timestamp: timestamp,
        )

        // Verify the object ID format follows RTO14
        #expect(operation.objectID.hasPrefix("map:"))
        #expect(operation.objectID.contains("@"))

        // Verify the ObjectMessage structure
        #expect(operation.objectMessage.operation?.action == .known(.mapCreate))
        #expect(operation.objectMessage.operation?.objectId == operation.objectID)
        #expect(operation.objectMessage.operation?.map?.semantics == .known(.lww))
        #expect(operation.objectMessage.operation?.nonce != nil)
        #expect(operation.objectMessage.operation?.initialValue != nil)

        // Verify map entries
        let mapEntries = operation.objectMessage.operation?.map?.entries
        #expect(mapEntries?.count == 4)

        // Check string value
        if case let .string(value) = mapEntries?["stringKey"]?.data.string {
            #expect(value == "stringValue")
        } else {
            Issue.record("Expected string value")
        }

        #expect(mapEntries?["numberKey"]?.data.number == NSNumber(value: 123.45))
        #expect(mapEntries?["boolKey"]?.data.boolean == true)
        #expect(mapEntries?["dataKey"]?.data.bytes == Data([1, 2, 3, 4]))
    }

    @Test
    func objectIDGeneration_followsRTO14Format() {
        let timestamp = Date(timeIntervalSince1970: 1_234_567_890)

        let counterOperation = ObjectCreationHelpers.creationOperationForLiveCounter(
            count: 42.0,
            timestamp: timestamp,
        )

        let mapOperation = ObjectCreationHelpers.creationOperationForLiveMap(
            entries: [:],
            timestamp: timestamp,
        )

        // Verify object ID format: [type]:[hash]@[timestamp]
        let counterPattern = #/^counter:[A-Za-z0-9_-]+@\d+$/#
        let mapPattern = #/^map:[A-Za-z0-9_-]+@\d+$/#

        #expect(counterOperation.objectID.wholeMatch(of: counterPattern) != nil)
        #expect(mapOperation.objectID.wholeMatch(of: mapPattern) != nil)

        // Verify timestamp part
        let expectedTimestamp = Int(timestamp.timeIntervalSince1970 * 1000)
        #expect(counterOperation.objectID.hasSuffix("@\(expectedTimestamp)"))
        #expect(mapOperation.objectID.hasSuffix("@\(expectedTimestamp)"))
    }

    @Test
    func nonceGeneration_createsUniqueValues() {
        let timestamp = Date()

        let operation1 = ObjectCreationHelpers.creationOperationForLiveCounter(
            count: 42.0,
            timestamp: timestamp,
        )

        let operation2 = ObjectCreationHelpers.creationOperationForLiveCounter(
            count: 42.0,
            timestamp: timestamp,
        )

        // Nonces should be different even with same input
        #expect(operation1.objectMessage.operation?.nonce != operation2.objectMessage.operation?.nonce)
        #expect(operation1.objectID != operation2.objectID)
    }
}
