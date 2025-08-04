import Foundation

/// Represents an execution of a test case method.
struct Test {
    var id = UUID()

    init() {
        if TestLogger.loggingEnabled {
            NSLog("Created test \(id)")
        }
    }
}
