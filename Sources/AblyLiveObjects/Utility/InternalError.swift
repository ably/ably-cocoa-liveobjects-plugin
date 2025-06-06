import Ably

/// An error thrown by the internals of the LiveObjects SDK.
///
/// Copied from ably-chat-swift; will decide what to do about it.
internal enum InternalError: Error {
    case errorInfo(ARTErrorInfo)
    case other(Other)

    internal enum Other {
        case jsonValueDecodingError(JSONValueDecodingError)
    }
}

internal extension ARTErrorInfo {
    func toInternalError() -> InternalError {
        .errorInfo(self)
    }
}

internal extension JSONValueDecodingError {
    func toInternalError() -> InternalError {
        .other(.jsonValueDecodingError(self))
    }
}
