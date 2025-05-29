import Ably

public extension ARTRealtimeChannel {
    /// An ``Objects`` object.
    var objects: Objects {
        PluginImplementation.objectsProperty(for: self)
    }
}
