internal import AblyPlugin

// We explicitly import the NSObject class, else it seems to get transitively imported from  `internal import AblyPlugin`, leading to the error "Class cannot be declared public because its superclass is internal".
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
/// // Fetch a channel, specifying the `.objectPublish` and `.objectSubscribe` modes
/// let channelOptions = ARTRealtimeChannelOptions()
/// channelOptions.modes = [.objectPublish, .objectSubscribe]
/// let channel = realtime.channels.get("myChannel", options: channelOptions)
///
/// // Attach the channel
/// try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
///     channel.attach { error in
///         if let error {
///             continuation.resume(throwing: error)
///         } else {
///             continuation.resume()
///         }
///     }
/// }
///
/// // You can now access LiveObjects functionality via the channel's `objects` property:
/// let rootObject = try await channel.objects.getRoot()
/// // …and so on
/// ```
@objc
public class Plugin: NSObject {
    /// The `AblyPlugin.PluginAPIProtocol` that the LiveObjects plugin should use by default (i.e. when one hasn't been injected for test purposes).
    internal static let defaultPluginAPI: AblyPlugin.PluginAPIProtocol = AblyPlugin.PluginAPI.sharedInstance()

    // MARK: - Informal conformance to AblyPlugin.LiveObjectsPluginProtocol

    @objc private static let internalPlugin = DefaultInternalPlugin(pluginAPI: defaultPluginAPI)
}
