internal import AblyPlugin
import CryptoKit
import Foundation

/// Helpers for creating a new LiveObject.
///
/// These generate an object ID and the `ObjectMessage` needed to create the LiveObject.
internal enum ObjectCreationHelpers {
    /// The metadata that a LiveObject creation method (e.g. `createCounter`) needs in order to request that Realtime create a LiveObject and to populate the local objects pool.
    internal struct ObjectCreationOperation {
        /// The generated object ID. Needed for populating the local objects pool.
        ///
        /// We include this property separately as a non-nil value, instead of expecting the caller to fish the nullable value out of ``objectMessage``.
        internal var objectID: String

        /// The operation that should be merged into any created LiveObject.
        ///
        /// We include this property separately as a non-nil value, instead of expecting the caller to fish the nullable value out of ``objectMessage``.
        internal var operation: ObjectOperation

        /// The ObjectMessage that must be sent in order for Realtime to create the object.
        internal var objectMessage: OutboundObjectMessage
    }

    /// Contains only the properties of `ObjectOperation` needed for creating the initial value JSON string. In particular, it does not have an `objectID`.
    internal struct LiveObjectInitialValue {
        internal var action: WireEnum<ObjectOperationAction>
        internal var map: ObjectsMap?
        internal var counter: WireObjectsCounter?
    }

    /// Creates a `COUNTER_CREATE` `ObjectMessage` for the `RealtimeObjects.createCounter` method per RTO12f.
    ///
    /// - Parameters:
    ///   - count: The initial count for the new LiveCounter object
    ///   - timestamp: The timestamp to use for the generated object ID.
    internal static func creationOperationForLiveCounter(
        count: Double,
        timestamp: Date,
    ) -> ObjectCreationOperation {
        // RTO12f2: Create initial value for the new LiveCounter
        let initialValue = LiveObjectInitialValue(
            action: .known(.counterCreate),
            counter: WireObjectsCounter(count: NSNumber(value: count)),
        )

        // RTO12f3: Create an initial value JSON string as described in RTO13
        let initialValueJSONString = createInitialValueJSONString(from: initialValue)

        // RTO12f4: Create a unique nonce as a random string
        let nonce = generateNonce()

        // RTO12f5: Get the current server time (using the provided timestamp)
        let serverTime = timestamp

        // RTO12f6: Create an objectId for the new LiveCounter object as described in RTO14
        let objectId = createObjectID(
            type: "counter",
            initialValue: initialValueJSONString,
            nonce: nonce,
            timestamp: serverTime,
        )

        // RTO12f7-12: Set ObjectMessage.operation fields
        let operation = ObjectOperation(
            action: .known(.counterCreate),
            objectId: objectId,
            counter: WireObjectsCounter(count: NSNumber(value: count)),
            nonce: nonce,
            initialValue: initialValueJSONString,
        )

        // Create the OutboundObjectMessage
        let objectMessage = OutboundObjectMessage(
            operation: operation,
        )

        return ObjectCreationOperation(
            objectID: objectId,
            operation: operation,
            objectMessage: objectMessage,
        )
    }

    /// Creates a `MAP_CREATE` `ObjectMessage` for the `RealtimeObjects.createMap` method per RTO11f.
    ///
    /// - Parameters:
    ///   - entries: The initial entries for the new LiveMap object
    ///   - timestamp: The timestamp to use for the generated object ID.
    internal static func creationOperationForLiveMap(
        entries: [String: InternalLiveMapValue],
        timestamp: Date,
    ) -> ObjectCreationOperation {
        // RTO11f4: Create initial value for the new LiveMap
        let mapEntries = entries.mapValues { liveMapValue -> ObjectsMapEntry in
            // RTO11f4c1: Create an ObjectsMapEntry for the current value
            let objectData = switch liveMapValue {
            case let .primitive(primitiveValue):
                switch primitiveValue {
                case let .bool(value):
                    ObjectData(boolean: value)
                case let .data(value):
                    ObjectData(bytes: value)
                case let .number(value):
                    ObjectData(number: NSNumber(value: value))
                case let .string(value):
                    ObjectData(string: .string(value))
                }
            case let .liveMap(liveMap):
                // RTO11f4c1a: If the value is of type LiveMap, set ObjectsMapEntry.data.objectId to the objectId of that object
                ObjectData(objectId: liveMap.objectID)
            case let .liveCounter(liveCounter):
                // RTO11f4c1a: If the value is of type LiveCounter, set ObjectsMapEntry.data.objectId to the objectId of that object
                ObjectData(objectId: liveCounter.objectID)
            }

            return ObjectsMapEntry(data: objectData)
        }

        let initialValue = LiveObjectInitialValue(
            action: .known(.mapCreate),
            map: ObjectsMap(
                semantics: .known(.lww),
                entries: mapEntries,
            ),
        )

        // RTO11f5: Create an initial value JSON string as described in RTO13
        let initialValueJSONString = createInitialValueJSONString(from: initialValue)

        // RTO11f6: Create a unique nonce as a random string
        let nonce = generateNonce()

        // RTO11f7: Get the current server time (using the provided timestamp)
        let serverTime = timestamp

        // RTO11f8: Create an objectId for the new LiveMap object as described in RTO14
        let objectId = createObjectID(
            type: "map",
            initialValue: initialValueJSONString,
            nonce: nonce,
            timestamp: serverTime,
        )

        // RTO11f9-13: Set ObjectMessage.operation fields
        let operation = ObjectOperation(
            action: .known(.mapCreate),
            objectId: objectId,
            map: ObjectsMap(
                semantics: .known(.lww),
                entries: mapEntries,
            ),
            nonce: nonce,
            initialValue: initialValueJSONString,
        )

        // Create the OutboundObjectMessage
        let objectMessage = OutboundObjectMessage(
            operation: operation,
        )

        return ObjectCreationOperation(
            objectID: objectId,
            operation: operation,
            objectMessage: objectMessage,
        )
    }

    // MARK: - Private Helper Methods

    /// Creates an initial value JSON string from a LiveObjectInitialValue, per RTO13.
    private static func createInitialValueJSONString(from initialValue: LiveObjectInitialValue) -> String {
        // RTO13b: Encode the initial value using OM4 encoding
        let jsonObject = initialValue.toWire(format: .json).toWireObject.mapValues { wireValue in
            do {
                return try wireValue.toJSONValue
            } catch {
                // By using `format: .json` we've requested a type that should be JSON-encodable, so if it isn't then it's a programmer error. (We can't reason about it statically though because of our choice to use a general-purpose WireValue type; maybe could improve upon this in the future.)
                preconditionFailure("Failed to convert WireValue \(wireValue) to JSONValue when encoding initialValue")
            }
        }

        // RTO13c
        return JSONObjectOrArray.object(jsonObject).toJSONString
    }

    /// Creates an Object ID for a new LiveObject instance, per RTO14.
    private static func createObjectID(
        type: String,
        initialValue: String,
        nonce: String,
        timestamp: Date,
    ) -> String {
        // RTO14b1: Generate a hash string for the Object ID
        let hashInput = "\(initialValue):\(nonce)"
        let hashData = Data(hashInput.utf8)

        // RTO14b1: Generate a SHA-256 digest
        let hash = SHA256.hash(data: hashData)

        // RTO14b2: Base64URL-encode the generated digest
        // TODO: check
        let base64URLHash = Data(hash).base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")

        // RTO14c: Return an Object ID in the format [type]:[hash]@[timestamp]
        let timestampMillis = Int(timestamp.timeIntervalSince1970 * 1000)
        return "\(type):\(base64URLHash)@\(timestampMillis)"
    }

    /// Generates a unique nonce as a random string, per RTO11f6 and RTO12f4.
    private static func generateNonce() -> String {
        // TODO: confirm if there's any specific rules here: https://github.com/ably/specification/pull/353/files#r2228252389
        let letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return String((0 ..< 16).map { _ in letters.randomElement()! })
    }
}

// MARK: - LiveObjectInitialValue Extensions

internal extension ObjectCreationHelpers.LiveObjectInitialValue {
    /// Converts this `LiveObjectInitialValue` to a `WireObjectOperation`, applying the data encoding rules of OD4 and OOP5.
    ///
    /// - Parameters:
    ///   - format: The format to use when applying the encoding rules of OD4 and OOP5.
    func toWire(format: AblyPlugin.EncodingFormat) -> WireObjectOperation {
        .init(
            action: action,
            // TODO: this is dodgy — we need a partial
            objectId: "", // Empty string since this is for initial value creation only
            mapOp: nil,
            counterOp: nil,
            map: map?.toWire(format: format),
            counter: counter,
            nonce: nil,
            initialValue: nil,
        )
    }
}
