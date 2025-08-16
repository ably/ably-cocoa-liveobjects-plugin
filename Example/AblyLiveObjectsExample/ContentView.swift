import Ably
import AblyLiveObjects
import SwiftUI

enum VoteColor: String, CaseIterable {
    case red = "red"
    case green = "green"
    case blue = "blue"
    
    var displayName: String {
        rawValue.capitalized
    }
    
    var swiftUIColor: SwiftUI.Color {
        switch self {
        case .red: return .red
        case .green: return .green
        case .blue: return .blue
        }
    }
}

@MainActor
class LiveCounterViewModel: ObservableObject {
    @Published var redCount: Double = 0
    @Published var greenCount: Double = 0
    @Published var blueCount: Double = 0
    @Published var isLoading = true
    @Published var errorMessage: String?
    
    private var realtime: ARTRealtime
    private var channel: ARTRealtimeChannel
    private var objects: any RealtimeObjects
    private var root: (any LiveMap)?
    
    private var redCounter: (any LiveCounter)?
    private var greenCounter: (any LiveCounter)?
    private var blueCounter: (any LiveCounter)?
    
    private var subscribeResponses: [any SubscribeResponse] = []
    
    init(realtime: ARTRealtime) {
        self.realtime = realtime
        
        // Use URL parameters or default channel name
        let channelName = "objects-live-counter"
        let channelOptions = ARTRealtimeChannelOptions()
        channelOptions.modes = [.objectPublish, .objectSubscribe]
        self.channel = realtime.channels.get(channelName, options: channelOptions)
        self.objects = channel.objects
        
        Task {
            await initializeCounters()
        }
    }
    
    deinit {
        // Clean up subscriptions
        subscribeResponses.forEach { $0.unsubscribe() }
        subscribeResponses.removeAll()
    }
    
    private func initializeCounters() async {
        do {
            isLoading = true
            errorMessage = nil
            
            // Attach channel first
            try await channel.attachAsync()
            
            // Get root object
            let root = try await objects.getRoot()
            self.root = root
            
            // Subscribe to root changes
            let rootSubscription = try root.subscribe { [weak self] update, _ in
                Task { @MainActor in
                    // Handle root updates - this will fire when counters are reset
                    for (keyName, change) in update.update {
                        if change == .updated, let color = VoteColor(rawValue: keyName) {
                            self?.subscribeToCounter(color: color)
                        }
                    }
                }
            }
            subscribeResponses.append(rootSubscription)
            
            // Initialize all color counters
            for color in VoteColor.allCases {
                await initializeCounter(for: color)
            }
            
            isLoading = false
        } catch {
            errorMessage = "Failed to initialize: \(error.localizedDescription)"
            isLoading = false
        }
    }
    
    private func initializeCounter(for color: VoteColor) async {
        do {
            guard let root = self.root else { return }
            
            // Check if counter already exists
            if let existingValue = try root.get(key: color.rawValue),
               let existingCounter = existingValue.liveCounterValue {
                // Counter exists, subscribe to it
                setCounter(existingCounter, for: color)
                subscribeToCounter(color: color)
            } else {
                // Counter doesn't exist, create it
                let newCounter = try await objects.createCounter(count: 0)
                try await root.set(key: color.rawValue, value: .liveCounter(newCounter))
                setCounter(newCounter, for: color)
            }
        } catch {
            errorMessage = "Failed to initialize \(color.rawValue) counter: \(error.localizedDescription)"
        }
    }
    
    private func setCounter(_ counter: any LiveCounter, for color: VoteColor) {
        do {
            let value = try counter.value
            switch color {
            case .red:
                redCounter = counter
                redCount = value
            case .green:
                greenCounter = counter
                greenCount = value
            case .blue:
                blueCounter = counter
                blueCount = value
            }
        } catch {
            errorMessage = "Error getting \(color.rawValue) counter value: \(error)"
        }
    }
    
    private func subscribeToCounter(color: VoteColor) {
        do {
            guard let root = self.root,
                  let value = try root.get(key: color.rawValue),
                  let counter = value.liveCounterValue else { return }
            
            let subscription = try counter.subscribe { [weak self] update, _ in
                Task { @MainActor in
                    self?.updateCounterValue(for: color, counter: counter)
                }
            }
            subscribeResponses.append(subscription)
            
            // Update current value
            updateCounterValue(for: color, counter: counter)
        } catch {
            errorMessage = "Failed to subscribe to \(color.rawValue) counter: \(error)"
        }
    }
    
    private func updateCounterValue(for color: VoteColor, counter: any LiveCounter) {
        do {
            let value = try counter.value
            switch color {
            case .red:
                redCount = value
            case .green:
                greenCount = value
            case .blue:
                blueCount = value
            }
        } catch {
            errorMessage = "Error updating \(color.rawValue) counter value: \(error)"
        }
    }
    
    func vote(for color: VoteColor) {
        Task {
            do {
                let counter: (any LiveCounter)?
                switch color {
                case .red: counter = redCounter
                case .green: counter = greenCounter
                case .blue: counter = blueCounter
                }
                
                try await counter?.increment(amount: 1)
            } catch {
                await MainActor.run {
                    errorMessage = "Failed to vote for \(color.rawValue): \(error.localizedDescription)"
                }
            }
        }
    }
    
    func resetCounters() {
        Task {
            do {
                guard let root = self.root else { return }
                
                // Create new counters for each color
                for color in VoteColor.allCases {
                    let newCounter = try await objects.createCounter(count: 0)
                    try await root.set(key: color.rawValue, value: .liveCounter(newCounter))
                }
            } catch {
                await MainActor.run {
                    errorMessage = "Failed to reset counters: \(error.localizedDescription)"
                }
            }
        }
    }
}

struct LiveCounterClientView: View {
    @ObservedObject var viewModel: LiveCounterViewModel
    let clientTitle: String
    
    var body: some View {
        VStack(spacing: 0) {
            if viewModel.isLoading {
                ProgressView("Loading...")
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                ScrollView {
                    VStack(spacing: 16) {
                        // Client identifier
                        Text(clientTitle)
                            .font(.system(size: 16, weight: .bold))
                            .foregroundColor(.secondary)
                        
                        // Card container
                        VStack(spacing: 2) {
                            // Header
                            Text("Vote for your favorite Color")
                                .font(.system(size: 14, weight: .bold))
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .padding(.bottom, 8)
                            
                            // Vote options
                            VStack(spacing: 0) {
                                ForEach(Array(VoteColor.allCases.enumerated()), id: \.offset) { index, color in
                                    VoteRow(
                                        color: color,
                                        count: countForColor(color),
                                        onVote: { viewModel.vote(for: color) }
                                    )
                                    
                                    if index < VoteColor.allCases.count - 1 {
                                        Divider()
                                            .background(SwiftUI.Color.gray.opacity(0.3))
                                    }
                                }
                            }
                            
                            // Reset button
                            Button(action: viewModel.resetCounters) {
                                Text("Reset")
                                    .font(.system(size: 14, weight: .medium))
                                    .foregroundColor(.primary)
                                    .padding(.horizontal, 16)
                                    .padding(.vertical, 8)
                            }
                            .padding(.top, 12)
                        }
                        .padding(24)
                        .background(.regularMaterial)
                        .cornerRadius(12)
                        .shadow(color: .black.opacity(0.1), radius: 8, x: 0, y: 2)
                        .frame(maxWidth: 320)
                        
                        // Error message
                        if let errorMessage = viewModel.errorMessage {
                            Text(errorMessage)
                                .foregroundColor(.red)
                                .font(.caption)
                                .padding()
                        }
                    }
                    .padding(24)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(.thickMaterial)
            }
        }
    }
    
    private func countForColor(_ color: VoteColor) -> Int {
        switch color {
        case .red: return Int(viewModel.redCount)
        case .green: return Int(viewModel.greenCount)
        case .blue: return Int(viewModel.blueCount)
        }
    }
}

struct ContentView: View {
    @StateObject private var viewModel1: LiveCounterViewModel
    @StateObject private var viewModel2: LiveCounterViewModel
    
    init(realtime1: ARTRealtime, realtime2: ARTRealtime) {
        self._viewModel1 = StateObject(wrappedValue: LiveCounterViewModel(realtime: realtime1))
        self._viewModel2 = StateObject(wrappedValue: LiveCounterViewModel(realtime: realtime2))
    }

    var body: some View {
        #if os(macOS) || os(tvOS)
        HStack(spacing: 1) {
            LiveCounterClientView(viewModel: viewModel1, clientTitle: "Client 1")
            
            Divider()
            
            LiveCounterClientView(viewModel: viewModel2, clientTitle: "Client 2")
        }
        #else
        VStack(spacing: 1) {
            LiveCounterClientView(viewModel: viewModel1, clientTitle: "Client 1")
            
            Divider()
            
            LiveCounterClientView(viewModel: viewModel2, clientTitle: "Client 2")
        }
        #endif
    }
}

struct VoteRow: View {
    let color: VoteColor
    let count: Int
    let onVote: () -> Void
    
    var body: some View {
        HStack(spacing: 16) {
            // Color name
            Text(color.displayName)
                .font(.system(size: 14, weight: .semibold))
                .foregroundColor(color.swiftUIColor)
                .frame(maxWidth: .infinity, alignment: .leading)
            
            // Count
            Text("\(count)")
                .font(.system(size: 14, weight: .bold))
                .foregroundColor(.primary)
            
            // Vote button
            Button(action: onVote) {
                Text("Vote")
                    .font(.system(size: 14, weight: .medium))
                    .foregroundColor(.primary)
                    .padding(.horizontal, 10)
                    .padding(.vertical, 4)
            }
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 12)
    }
}

extension ARTRealtimeChannelProtocol {
    func attachAsync() async throws(ARTErrorInfo) {
        try await withCheckedContinuation { (continuation: CheckedContinuation<Result<Void, ARTErrorInfo>, _>) in
            attach { error in
                if let error {
                    continuation.resume(returning: .failure(error))
                } else {
                    continuation.resume(returning: .success(()))
                }
            }
        }.get()
    }

    func detachAsync() async throws(ARTErrorInfo) {
        try await withCheckedContinuation { (continuation: CheckedContinuation<Result<Void, ARTErrorInfo>, _>) in
            detach { error in
                if let error {
                    continuation.resume(returning: .failure(error))
                } else {
                    continuation.resume(returning: .success(()))
                }
            }
        }.get()
    }
}
