import Ably
import AblyLiveObjects
import SwiftUI

@main
struct AblyLiveObjectsExampleApp: App {
    private func getRealtime() -> ARTRealtime {
        let clientOptions = ARTClientOptions(key: Secrets.ablyAPIKey)
        clientOptions.plugins = [.liveObjects: AblyLiveObjects.Plugin.self]
        return ARTRealtime(options: clientOptions)
    }

    var body: some Scene {
        WindowGroup {
            ContentView(realtime1: getRealtime(), realtime2: getRealtime())
        }
    }
}
