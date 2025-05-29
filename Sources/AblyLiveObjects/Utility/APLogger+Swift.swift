internal import AblyPlugin

internal extension APLogger {
    /// A convenience method that provides default values for `file` and `line`.
    func log(_ message: String, level: ARTLogLevel, fileID: String = #fileID, line: Int = #line) {
        log(message, with: level, file: fileID, line: line)
    }
}
