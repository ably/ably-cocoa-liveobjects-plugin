import Foundation

/// Same as the public ``LiveMapValue`` type but with associated values of internal type.
internal enum InternalLiveMapValue: Sendable {
    case primitive(PrimitiveObjectValue)
    case liveMap(InternalDefaultLiveMap)
    case liveCounter(InternalDefaultLiveCounter)

    // MARK: - Creating from a public LiveMapValue

    /// Converts a public ``LiveMapValue`` into an ``InternalLiveMapValue``.
    ///
    /// Needed in order to access the internals of user-provided LiveObject-valued LiveMap entries to extract their object ID.
    internal init(liveMapValue: LiveMapValue) {
        switch liveMapValue {
        case let .primitive(primitiveValue):
            self = .primitive(primitiveValue)
        case let .liveMap(publicLiveMap):
            guard let publicDefaultLiveMap = publicLiveMap as? PublicDefaultLiveMap else {
                // TODO: Try and remove this runtime check and know this type statically, see https://github.com/ably/ably-cocoa-liveobjects-plugin/issues/37
                preconditionFailure("Expected PublicDefaultLiveMap, got \(publicLiveMap)")
            }
            self = .liveMap(publicDefaultLiveMap.proxied)
        case let .liveCounter(publicLiveCounter):
            guard let publicDefaultLiveCounter = publicLiveCounter as? PublicDefaultLiveCounter else {
                // TODO: Try and remove this runtime check and know this type statically, see https://github.com/ably/ably-cocoa-liveobjects-plugin/issues/37
                preconditionFailure("Expected PublicDefaultLiveCounter, got \(publicLiveCounter)")
            }
            self = .liveCounter(publicDefaultLiveCounter.proxied)
        }
    }

    // MARK: - Convenience getters for associated values

    /// If this `InternalLiveMapValue` has case `primitive`, this returns the associated value. Else, it returns `nil`.
    internal var primitiveValue: PrimitiveObjectValue? {
        if case let .primitive(value) = self {
            return value
        }
        return nil
    }

    /// If this `InternalLiveMapValue` has case `liveMap`, this returns the associated value. Else, it returns `nil`.
    internal var liveMapValue: InternalDefaultLiveMap? {
        if case let .liveMap(value) = self {
            return value
        }
        return nil
    }

    /// If this `InternalLiveMapValue` has case `liveCounter`, this returns the associated value. Else, it returns `nil`.
    internal var liveCounterValue: InternalDefaultLiveCounter? {
        if case let .liveCounter(value) = self {
            return value
        }
        return nil
    }

    /// If this `InternalLiveMapValue` has case `primitive` with a string value, this returns that value. Else, it returns `nil`.
    internal var stringValue: String? {
        primitiveValue?.stringValue
    }

    /// If this `InternalLiveMapValue` has case `primitive` with a number value, this returns that value. Else, it returns `nil`.
    internal var numberValue: Double? {
        primitiveValue?.numberValue
    }

    /// If this `InternalLiveMapValue` has case `primitive` with a boolean value, this returns that value. Else, it returns `nil`.
    internal var boolValue: Bool? {
        primitiveValue?.boolValue
    }

    /// If this `InternalLiveMapValue` has case `primitive` with a data value, this returns that value. Else, it returns `nil`.
    internal var dataValue: Data? {
        primitiveValue?.dataValue
    }
}
