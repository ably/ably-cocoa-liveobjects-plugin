import Ably
import Foundation

internal protocol JSONEncodable {
    var toJSONValue: JSONValue { get }
}

internal protocol JSONDecodable {
    init(jsonValue: JSONValue) throws(InternalError)
}

internal typealias JSONCodable = JSONDecodable & JSONEncodable

internal protocol JSONObjectEncodable: JSONEncodable {
    var toJSONObject: [String: JSONValue] { get }
}

// Default implementation of `JSONEncodable` conformance for `JSONObjectEncodable`
internal extension JSONObjectEncodable {
    var toJSONValue: JSONValue {
        .object(toJSONObject)
    }
}

internal protocol JSONObjectDecodable: JSONDecodable {
    init(jsonObject: [String: JSONValue]) throws(InternalError)
}

internal enum JSONValueDecodingError: Error {
    case valueIsNotObject
    case noValueForKey(String)
    case wrongTypeForKey(String, actualValue: JSONValue)
    case failedToDecodeFromRawValue(String)
}

// Default implementation of `JSONDecodable` conformance for `JSONObjectDecodable`
internal extension JSONObjectDecodable {
    init(jsonValue: JSONValue) throws(InternalError) {
        guard case let .object(jsonObject) = jsonValue else {
            throw JSONValueDecodingError.valueIsNotObject.toInternalError()
        }

        self = try .init(jsonObject: jsonObject)
    }
}

internal typealias JSONObjectCodable = JSONObjectDecodable & JSONObjectEncodable

// MARK: - Extracting primitive values from a dictionary

/// This extension adds some helper methods for extracting values from a dictionary of `JSONValue` values; you may find them helpful when implementing `JSONCodable`.
internal extension [String: JSONValue] {
    /// If this dictionary contains a value for `key`, and this value has case `object`, this returns the associated value.
    ///
    /// - Throws:
    ///   - `JSONValueDecodingError.noValueForKey` if the key is absent
    ///   - `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `object`
    func objectValueForKey(_ key: String) throws(InternalError) -> [String: JSONValue] {
        guard let value = self[key] else {
            throw JSONValueDecodingError.noValueForKey(key).toInternalError()
        }

        guard case let .object(objectValue) = value else {
            throw JSONValueDecodingError.wrongTypeForKey(key, actualValue: value).toInternalError()
        }

        return objectValue
    }

    /// If this dictionary contains a value for `key`, and this value has case `object`, this returns the associated value. If this dictionary does not contain a value for `key`, or if the value for `key` has case `null`, it returns `nil`.
    ///
    /// - Throws: `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `object` or `null`
    func optionalObjectValueForKey(_ key: String) throws(InternalError) -> [String: JSONValue]? {
        guard let value = self[key] else {
            return nil
        }

        if case .null = value {
            return nil
        }

        guard case let .object(objectValue) = value else {
            throw JSONValueDecodingError.wrongTypeForKey(key, actualValue: value).toInternalError()
        }

        return objectValue
    }

    /// If this dictionary contains a value for `key`, and this value has case `array`, this returns the associated value.
    ///
    /// - Throws:
    ///   - `JSONValueDecodingError.noValueForKey` if the key is absent
    ///   - `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `array`
    func arrayValueForKey(_ key: String) throws(InternalError) -> [JSONValue] {
        guard let value = self[key] else {
            throw JSONValueDecodingError.noValueForKey(key).toInternalError()
        }

        guard case let .array(arrayValue) = value else {
            throw JSONValueDecodingError.wrongTypeForKey(key, actualValue: value).toInternalError()
        }

        return arrayValue
    }

    /// If this dictionary contains a value for `key`, and this value has case `array`, this returns the associated value. If this dictionary does not contain a value for `key`, or if the value for `key` has case `null`, it returns `nil`.
    ///
    /// - Throws: `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `array` or `null`
    func optionalArrayValueForKey(_ key: String) throws(InternalError) -> [JSONValue]? {
        guard let value = self[key] else {
            return nil
        }

        if case .null = value {
            return nil
        }

        guard case let .array(arrayValue) = value else {
            throw JSONValueDecodingError.wrongTypeForKey(key, actualValue: value).toInternalError()
        }

        return arrayValue
    }

    /// If this dictionary contains a value for `key`, and this value has case `string`, this returns the associated value.
    ///
    /// - Throws:
    ///   - `JSONValueDecodingError.noValueForKey` if the key is absent
    ///   - `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `string`
    func stringValueForKey(_ key: String) throws(InternalError) -> String {
        guard let value = self[key] else {
            throw JSONValueDecodingError.noValueForKey(key).toInternalError()
        }

        guard case let .string(stringValue) = value else {
            throw JSONValueDecodingError.wrongTypeForKey(key, actualValue: value).toInternalError()
        }

        return stringValue
    }

    /// If this dictionary contains a value for `key`, and this value has case `string`, this returns the associated value. If this dictionary does not contain a value for `key`, or if the value for `key` has case `null`, it returns `nil`.
    ///
    /// - Throws: `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `string` or `null`
    func optionalStringValueForKey(_ key: String) throws(InternalError) -> String? {
        guard let value = self[key] else {
            return nil
        }

        if case .null = value {
            return nil
        }

        guard case let .string(stringValue) = value else {
            throw JSONValueDecodingError.wrongTypeForKey(key, actualValue: value).toInternalError()
        }

        return stringValue
    }

    /// If this dictionary contains a value for `key`, and this value has case `number`, this returns the associated value.
    ///
    /// - Throws:
    ///   - `JSONValueDecodingError.noValueForKey` if the key is absent
    ///   - `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `number`
    func numberValueForKey(_ key: String) throws(InternalError) -> NSNumber {
        guard let value = self[key] else {
            throw JSONValueDecodingError.noValueForKey(key).toInternalError()
        }

        guard case let .number(numberValue) = value else {
            throw JSONValueDecodingError.wrongTypeForKey(key, actualValue: value).toInternalError()
        }

        return numberValue
    }

    /// If this dictionary contains a value for `key`, and this value has case `number`, this returns the associated value. If this dictionary does not contain a value for `key`, or if the value for `key` has case `null`, it returns `nil`.
    ///
    /// - Throws: `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `number` or `null`
    func optionalNumberValueForKey(_ key: String) throws(InternalError) -> NSNumber? {
        guard let value = self[key] else {
            return nil
        }

        if case .null = value {
            return nil
        }

        guard case let .number(numberValue) = value else {
            throw JSONValueDecodingError.wrongTypeForKey(key, actualValue: value).toInternalError()
        }

        return numberValue
    }

    /// If this dictionary contains a value for `key`, and this value has case `bool`, this returns the associated value.
    ///
    /// - Throws:
    ///   - `JSONValueDecodingError.noValueForKey` if the key is absent
    ///   - `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `bool`
    func boolValueForKey(_ key: String) throws(InternalError) -> Bool {
        guard let value = self[key] else {
            throw JSONValueDecodingError.noValueForKey(key).toInternalError()
        }

        guard case let .bool(boolValue) = value else {
            throw JSONValueDecodingError.wrongTypeForKey(key, actualValue: value).toInternalError()
        }

        return boolValue
    }

    /// If this dictionary contains a value for `key`, and this value has case `bool`, this returns the associated value. If this dictionary does not contain a value for `key`, or if the value for `key` has case `null`, it returns `nil`.
    ///
    /// - Throws: `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `bool` or `null`
    func optionalBoolValueForKey(_ key: String) throws(InternalError) -> Bool? {
        guard let value = self[key] else {
            return nil
        }

        if case .null = value {
            return nil
        }

        guard case let .bool(boolValue) = value else {
            throw JSONValueDecodingError.wrongTypeForKey(key, actualValue: value).toInternalError()
        }

        return boolValue
    }
}

// MARK: - Extracting dates from a dictionary

internal extension [String: JSONValue] {
    /// If this dictionary contains a value for `key`, and this value has case `number`, this returns a date created by interpreting this value as the number of milliseconds since the Unix epoch (which is the format used by Ably).
    ///
    /// - Throws:
    ///   - `JSONValueDecodingError.noValueForKey` if the key is absent
    ///   - `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `number`
    func ablyProtocolDateValueForKey(_ key: String) throws(InternalError) -> Date {
        let millisecondsSinceEpoch = try numberValueForKey(key).uint64Value

        return dateFromMillisecondsSinceEpoch(millisecondsSinceEpoch)
    }

    /// If this dictionary contains a value for `key`, and this value has case `number`, this returns a date created by interpreting this value as the number of milliseconds since the Unix epoch (which is the format used by Ably). If this dictionary does not contain a value for `key`, or if the value for `key` has case `null`, it returns `nil`.
    ///
    /// - Throws: `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `number` or `null`
    func optionalAblyProtocolDateValueForKey(_ key: String) throws(InternalError) -> Date? {
        guard let millisecondsSinceEpoch = try optionalNumberValueForKey(key)?.uint64Value else {
            return nil
        }
        return dateFromMillisecondsSinceEpoch(millisecondsSinceEpoch)
    }

    private func dateFromMillisecondsSinceEpoch(_ millisecondsSinceEpoch: UInt64) -> Date {
        .init(timeIntervalSince1970: Double(millisecondsSinceEpoch) / 1000)
    }
}

// MARK: - Extracting RawRepresentable values from a dictionary

internal extension [String: JSONValue] {
    /// If this dictionary contains a value for `key`, and this value has case `string`, this creates an instance of `T` using its `init(rawValue:)` initializer.
    ///
    /// - Throws:
    ///   - `JSONValueDecodingError.noValueForKey` if the key is absent
    ///   - `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `string`
    ///   - `JSONValueDecodingError.failedToDecodeFromRawValue` if `init(rawValue:)` returns `nil`
    func rawRepresentableValueForKey<T: RawRepresentable>(_ key: String, type _: T.Type = T.self) throws(InternalError) -> T where T.RawValue == String {
        let rawValue = try stringValueForKey(key)

        return try rawRepresentableValueFromRawValue(rawValue, type: T.self)
    }

    /// If this dictionary contains a value for `key`, and this value has case `string`, this creates an instance of `T` using its `init(rawValue:)` initializer. If this dictionary does not contain a value for `key`, or if the value for `key` has case `null`, it returns `nil`.
    ///
    /// - Throws:
    ///   - `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `string` or `null`
    ///   - `JSONValueDecodingError.failedToDecodeFromRawValue` if `init(rawValue:)` returns `nil`
    func optionalRawRepresentableValueForKey<T: RawRepresentable>(_ key: String, type _: T.Type = T.self) throws(InternalError) -> T? where T.RawValue == String {
        guard let rawValue = try optionalStringValueForKey(key) else {
            return nil
        }

        return try rawRepresentableValueFromRawValue(rawValue, type: T.self)
    }

    private func rawRepresentableValueFromRawValue<T: RawRepresentable>(_ rawValue: String, type _: T.Type = T.self) throws(InternalError) -> T where T.RawValue == String {
        guard let value = T(rawValue: rawValue) else {
            throw JSONValueDecodingError.failedToDecodeFromRawValue(rawValue).toInternalError()
        }

        return value
    }
}

// MARK: - Extracting WireEnum values from a dictionary

internal extension [String: JSONValue] {
    /// If this dictionary contains a value for `key`, and this value has case `number`, this creates a `WireEnum` instance using its `init(rawValue:)` initializer.
    ///
    /// - Throws:
    ///   - `JSONValueDecodingError.noValueForKey` if the key is absent
    ///   - `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `number`
    func wireEnumValueForKey<Known: RawRepresentable>(_ key: String, type _: Known.Type = Known.self) throws(InternalError) -> WireEnum<Known> where Known.RawValue == Int {
        let rawValue = try numberValueForKey(key).intValue
        return WireEnum(rawValue: rawValue)
    }

    /// If this dictionary contains a value for `key`, and this value has case `number`, this creates a `WireEnum` instance using its `init(rawValue:)` initializer. If this dictionary does not contain a value for `key`, or if the value for `key` has case `null`, it returns `nil`.
    ///
    /// - Throws: `JSONValueDecodingError.wrongTypeForKey` if the value does not have case `number` or `null`
    func optionalWireEnumValueForKey<Known: RawRepresentable>(_ key: String, type _: Known.Type = Known.self) throws(InternalError) -> WireEnum<Known>? where Known.RawValue == Int {
        guard let rawValue = try optionalNumberValueForKey(key)?.intValue else {
            return nil
        }
        return WireEnum(rawValue: rawValue)
    }
}

// MARK: - Extracting JSONDecodable values from a dictionary

internal extension [String: JSONValue] {
    /// If this dictionary contains a value for `key`, this attempts to decode it into an instance of `T` using its `init(jsonValue:)` initializer.
    ///
    /// - Throws:
    ///   - `JSONValueDecodingError.noValueForKey` if the key is absent
    ///   - Any error thrown by `T.init(jsonValue:)`
    func decodableValueForKey<T: JSONDecodable>(_ key: String, type _: T.Type = T.self) throws(InternalError) -> T {
        guard let value = self[key] else {
            throw JSONValueDecodingError.noValueForKey(key).toInternalError()
        }

        return try T(jsonValue: value)
    }

    /// If this dictionary contains a value for `key`, this attempts to decode it into an instance of `T` using its `init(jsonValue:)` initializer. If this dictionary does not contain a value for `key`, or if the value for `key` has case `null`, it returns `nil`.
    ///
    /// - Throws: Any error thrown by `T.init(jsonValue:)`
    func optionalDecodableValueForKey<T: JSONDecodable>(_ key: String, type _: T.Type = T.self) throws(InternalError) -> T? {
        guard let value = self[key] else {
            return nil
        }

        if case .null = value {
            return nil
        }

        return try T(jsonValue: value)
    }
}
