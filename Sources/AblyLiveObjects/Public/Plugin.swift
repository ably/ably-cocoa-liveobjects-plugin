internal import _AblyPluginSupportPrivate

// We explicitly import the NSObject class, else it seems to get transitively imported from  `internal import _AblyPluginSupportPrivate`, leading to the error "Class cannot be declared public because its superclass is internal".
import ObjectiveC.NSObject

/// This plugin enables LiveObjects functionality in ably-cocoa. Set the `.liveObjects` key in the ably-cocoa `plugins` client option to this class in order to enable LiveObjects.
///
/// For example:
/// ```swift
/// import Ably
/// import AblyLiveObjects
///
/// let clientOptions = ARTClientOptions(key: /* <insert your Ably API key here> */)
/// clientOptions.plugins = [.liveObjects: AblyLiveObjects.Plugin.self]
///
/// let realtime = ARTRealtime(options: clientOptions)
///
/// // You can now access LiveObjects functionality via a channel's `objects` property:
/// let channel = realtime.channels.get("myChannel")
/// let rootObject = try await channel.objects.getRoot()
/// // …and so on
/// ```
@objc
public class Plugin: NSObject {
    /// The `_AblyPluginSupportPrivate.PluginAPIProtocol` that the LiveObjects plugin should use by default (i.e. when one hasn't been injected for test purposes).
    internal static let defaultPluginAPI = _AblyPluginSupportPrivate.DependencyStore.sharedInstance().fetchPluginAPI()

    // MARK: - Informal conformance to _AblyPluginSupportPrivate.LiveObjectsPluginProtocol

    @objc private static let internalPlugin = DefaultInternalPlugin(pluginAPI: defaultPluginAPI)
}
