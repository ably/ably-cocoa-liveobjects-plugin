import Foundation

/// An `ObjectMessage` received or to be sent in the `state` property of an `OBJECT` or `OBJECT_SYNC` `ProtocolMessage`.
internal struct WireObjectMessage {
//    var id: String // OM2a — TODO implement, needs cooperation from ably-cocoa
    internal var clientId: String // OM2b
//    var connectionId: String // OM2c — TODO implement, needs cooperation from ably-cocoa
    internal var extras: [String: JSONValue]? // OM2d
//     var timestamp: Date // OM2e — TODO implement, needs cooperation from ably-cocoa
    internal var operation: WireObjectOperation? // OM2f
    internal var object: WireObjectState? // OM2g
    internal var serial: String // OM2h
    internal var siteCode: String // OM2i
}

extension WireObjectMessage: JSONObjectCodable {
    internal enum JSONKey: String {
        case clientId
        case extras
        case operation
        case object
        case serial
        case siteCode
    }

    internal init(jsonObject: [String: JSONValue]) throws(InternalError) {
        clientId = try jsonObject.stringValueForKey(JSONKey.clientId.rawValue)
        extras = try jsonObject.objectValueForKey(JSONKey.extras.rawValue)
        operation = try jsonObject.optionalDecodableValueForKey(JSONKey.operation.rawValue)
        object = try jsonObject.optionalDecodableValueForKey(JSONKey.object.rawValue)
        serial = try jsonObject.stringValueForKey(JSONKey.serial.rawValue)
        siteCode = try jsonObject.stringValueForKey(JSONKey.siteCode.rawValue)
    }

    internal var toJSONObject: [String: JSONValue] {
        let extrasValue: JSONValue? = if let extras {
            .object(extras)
        } else {
            nil
        }

        let operationValue: JSONValue? = if let operation {
            .object(operation.toJSONObject)
        } else {
            nil
        }

        let objectValue: JSONValue? = if let object {
            .object(object.toJSONObject)
        } else {
            nil
        }

        return [
            JSONKey.clientId.rawValue: .string(clientId),
            JSONKey.extras.rawValue: extrasValue,
            JSONKey.operation.rawValue: operationValue,
            JSONKey.object.rawValue: objectValue,
            JSONKey.serial.rawValue: .string(serial),
            JSONKey.siteCode.rawValue: .string(siteCode),
        ].compactMapValues { $0 }
    }
}

// OOP2
internal enum ObjectOperationAction: Int {
    case mapCreate = 0
    case mapSet = 1
    case mapRemove = 2
    case counterCreate = 3
    case counterInc = 4
    case objectDelete = 5
}

// MAP2
internal enum MapSemantics: Int {
    case lww = 0
}

internal struct WireObjectOperation {
    internal var action: WireEnum<ObjectOperationAction> // OOP3a
    internal var objectId: String // OOP3b
    internal var mapOp: WireMapOp? // OOP3c
    internal var counterOp: WireCounterOp? // OOP3d
    internal var map: WireMap? // OOP3e
    internal var counter: WireCounter? // OOP3f
    internal var nonce: String? // OOP3g
    // TODO: Implement initialValue
    internal var initialValueEncoding: String? // OOP3i
}

extension WireObjectOperation: JSONObjectCodable {
    internal enum JSONKey: String {
        case action
        case objectId
        case mapOp
        case counterOp
        case map
        case counter
        case nonce
        case initialValueEncoding
    }

    internal init(jsonObject: [String: JSONValue]) throws(InternalError) {
        action = try jsonObject.wireEnumValueForKey(JSONKey.action.rawValue)
        objectId = try jsonObject.stringValueForKey(JSONKey.objectId.rawValue)
        mapOp = try jsonObject.optionalDecodableValueForKey(JSONKey.mapOp.rawValue)
        counterOp = try jsonObject.optionalDecodableValueForKey(JSONKey.counterOp.rawValue)
        map = try jsonObject.optionalDecodableValueForKey(JSONKey.map.rawValue)
        counter = try jsonObject.optionalDecodableValueForKey(JSONKey.counter.rawValue)
        nonce = try jsonObject.optionalStringValueForKey(JSONKey.nonce.rawValue)
        initialValueEncoding = try jsonObject.optionalStringValueForKey(JSONKey.initialValueEncoding.rawValue)
    }

    internal var toJSONObject: [String: JSONValue] {
        var result: [String: JSONValue] = [
            JSONKey.action.rawValue: .number(Double(action.rawValue)),
            JSONKey.objectId.rawValue: .string(objectId),
        ]

        if let mapOp {
            result[JSONKey.mapOp.rawValue] = .object(mapOp.toJSONObject)
        }
        if let counterOp {
            result[JSONKey.counterOp.rawValue] = .object(counterOp.toJSONObject)
        }
        if let map {
            result[JSONKey.map.rawValue] = .object(map.toJSONObject)
        }
        if let counter {
            result[JSONKey.counter.rawValue] = .object(counter.toJSONObject)
        }
        if let nonce {
            result[JSONKey.nonce.rawValue] = .string(nonce)
        }
        if let initialValueEncoding {
            result[JSONKey.initialValueEncoding.rawValue] = .string(initialValueEncoding)
        }

        return result
    }
}

internal struct WireObjectState {
    internal var objectId: String // OST2a
    internal var siteTimeserials: [String: String] // OST2b
    internal var tombstone: Bool // OST2c
    internal var createOp: WireObjectOperation? // OST2d
    internal var map: WireMap? // OST2e
    internal var counter: WireCounter? // OST2f
}

extension WireObjectState: JSONObjectCodable {
    internal enum JSONKey: String {
        case objectId
        case siteTimeserials
        case tombstone
        case createOp
        case map
        case counter
    }

    internal init(jsonObject: [String: JSONValue]) throws(InternalError) {
        objectId = try jsonObject.stringValueForKey(JSONKey.objectId.rawValue)
        siteTimeserials = try jsonObject.objectValueForKey(JSONKey.siteTimeserials.rawValue).ablyLiveObjects_mapValuesWithTypedThrow { value throws(InternalError) in
            guard case let .string(string) = value else {
                throw JSONValueDecodingError.wrongTypeForKey(JSONKey.siteTimeserials.rawValue, actualValue: value).toInternalError()
            }
            return string
        }
        tombstone = try jsonObject.boolValueForKey(JSONKey.tombstone.rawValue)
        createOp = try jsonObject.optionalDecodableValueForKey(JSONKey.createOp.rawValue)
        map = try jsonObject.optionalDecodableValueForKey(JSONKey.map.rawValue)
        counter = try jsonObject.optionalDecodableValueForKey(JSONKey.counter.rawValue)
    }

    internal var toJSONObject: [String: JSONValue] {
        var result: [String: JSONValue] = [
            JSONKey.objectId.rawValue: .string(objectId),
            JSONKey.siteTimeserials.rawValue: .object(siteTimeserials.mapValues { .string($0) }),
            JSONKey.tombstone.rawValue: .bool(tombstone),
        ]

        if let createOp {
            result[JSONKey.createOp.rawValue] = .object(createOp.toJSONObject)
        }
        if let map {
            result[JSONKey.map.rawValue] = .object(map.toJSONObject)
        }
        if let counter {
            result[JSONKey.counter.rawValue] = .object(counter.toJSONObject)
        }

        return result
    }
}

internal struct WireMapOp {
    internal var key: String // MOP2a
    internal var data: WireObjectData? // MOP2b
}

extension WireMapOp: JSONObjectCodable {
    internal enum JSONKey: String {
        case key
        case data
    }

    internal init(jsonObject: [String: JSONValue]) throws(InternalError) {
        key = try jsonObject.stringValueForKey(JSONKey.key.rawValue)
        data = try jsonObject.optionalDecodableValueForKey(JSONKey.data.rawValue)
    }

    internal var toJSONObject: [String: JSONValue] {
        var result: [String: JSONValue] = [
            JSONKey.key.rawValue: .string(key),
        ]

        if let data {
            result[JSONKey.data.rawValue] = .object(data.toJSONObject)
        }

        return result
    }
}

internal struct WireCounterOp {
    internal var amount: Double // COP2a
}

extension WireCounterOp: JSONObjectCodable {
    internal enum JSONKey: String {
        case amount
    }

    internal init(jsonObject: [String: JSONValue]) throws(InternalError) {
        amount = try jsonObject.numberValueForKey(JSONKey.amount.rawValue)
    }

    internal var toJSONObject: [String: JSONValue] {
        [
            JSONKey.amount.rawValue: .number(amount),
        ]
    }
}

internal struct WireMap {
    internal var semantics: WireEnum<MapSemantics> // MAP3a
    internal var entries: [String: WireMapEntry]? // MAP3b
}

extension WireMap: JSONObjectCodable {
    internal enum JSONKey: String {
        case semantics
        case entries
    }

    internal init(jsonObject: [String: JSONValue]) throws(InternalError) {
        semantics = try jsonObject.wireEnumValueForKey(JSONKey.semantics.rawValue)
        entries = try jsonObject.optionalObjectValueForKey(JSONKey.entries.rawValue)?.ablyLiveObjects_mapValuesWithTypedThrow { value throws(InternalError) in
            guard case let .object(object) = value else {
                throw JSONValueDecodingError.wrongTypeForKey(JSONKey.entries.rawValue, actualValue: value).toInternalError()
            }
            return try WireMapEntry(jsonObject: object)
        }
    }

    internal var toJSONObject: [String: JSONValue] {
        var result: [String: JSONValue] = [
            JSONKey.semantics.rawValue: .number(Double(semantics.rawValue)),
        ]

        if let entries {
            result[JSONKey.entries.rawValue] = .object(entries.mapValues { .object($0.toJSONObject) })
        }

        return result
    }
}

internal struct WireCounter {
    internal var count: Double? // CNT2a
}

extension WireCounter: JSONObjectCodable {
    internal enum JSONKey: String {
        case count
    }

    internal init(jsonObject: [String: JSONValue]) throws(InternalError) {
        count = try jsonObject.optionalNumberValueForKey(JSONKey.count.rawValue)
    }

    internal var toJSONObject: [String: JSONValue] {
        var result: [String: JSONValue] = [:]
        if let count {
            result[JSONKey.count.rawValue] = .number(count)
        }
        return result
    }
}

internal struct WireMapEntry {
    internal var tombstone: Bool? // ME2a
    internal var timeserial: String? // ME2b
    internal var data: WireObjectData // ME2c
}

extension WireMapEntry: JSONObjectCodable {
    internal enum JSONKey: String {
        case tombstone
        case timeserial
        case data
    }

    internal init(jsonObject: [String: JSONValue]) throws(InternalError) {
        tombstone = try jsonObject.optionalBoolValueForKey(JSONKey.tombstone.rawValue)
        timeserial = try jsonObject.optionalStringValueForKey(JSONKey.timeserial.rawValue)
        data = try jsonObject.decodableValueForKey(JSONKey.data.rawValue)
    }

    internal var toJSONObject: [String: JSONValue] {
        var result: [String: JSONValue] = [
            JSONKey.data.rawValue: .object(data.toJSONObject),
        ]

        if let tombstone {
            result[JSONKey.tombstone.rawValue] = .bool(tombstone)
        }
        if let timeserial {
            result[JSONKey.timeserial.rawValue] = .string(timeserial)
        }

        return result
    }
}

internal struct WireObjectData {
    internal var objectId: String? // OD2a
    internal var encoding: String? // OD2b
    // TODO: Implement `value`
}

extension WireObjectData: JSONObjectCodable {
    internal enum JSONKey: String {
        case objectId
        case encoding
    }

    internal init(jsonObject: [String: JSONValue]) throws(InternalError) {
        objectId = try jsonObject.optionalStringValueForKey(JSONKey.objectId.rawValue)
        encoding = try jsonObject.optionalStringValueForKey(JSONKey.encoding.rawValue)
    }

    internal var toJSONObject: [String: JSONValue] {
        var result: [String: JSONValue] = [:]

        if let objectId {
            result[JSONKey.objectId.rawValue] = .string(objectId)
        }
        if let encoding {
            result[JSONKey.encoding.rawValue] = .string(encoding)
        }

        return result
    }
}
