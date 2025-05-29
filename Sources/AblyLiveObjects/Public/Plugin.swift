// We explicitly import the NSObject class, else it seems to get transitively imported from  `internal import AblyPlugin`, leading to the error "Class cannot be declared public because its superclass is internal".
internal import AblyPlugin
import ObjectiveC.NSObject

/// The class that should be passed in the `plugins` client option to enable LiveObjects.
@objc
public class Plugin: NSObject {
    // This class informally conforms to AblyPlugin.LiveObjectsPluginFactoryProtocol

    @objc
    private static func createPlugin() -> PluginImplementation {
        PluginImplementation()
    }
}

@objc
internal class PluginImplementation: NSObject, AblyPlugin.LiveObjectsPluginProtocol {
    @objc
    private final class Box: NSObject {
        var value: DefaultLiveObjects

        init(value: DefaultLiveObjects) {
            self.value = value
        }
    }

    private static let pluginDataKey = "LiveObjects"

    internal func prepare(_ channel: ARTRealtimeChannel) {
        print("LiveObjects.Plugin received prepare(_:)")
        let liveObjects = DefaultLiveObjects(channel: channel)
        let box = Box(value: liveObjects)
        AblyPlugin.PluginAPI.setPluginDataValue(box, forKey: Self.pluginDataKey, channel: channel)
    }

    internal static func objectsProperty(for channel: ARTRealtimeChannel) -> DefaultLiveObjects {
        guard let pluginData = AblyPlugin.PluginAPI.pluginDataValue(forKey: pluginDataKey, channel: channel) else {
            // Plugin.prepare was not called
            fatalError("You must pass AblyLiveObjects.Plugin in the ClientOptions")
        }

        // swiftlint:disable:next force_cast
        return (pluginData as! Box).value
    }
}
