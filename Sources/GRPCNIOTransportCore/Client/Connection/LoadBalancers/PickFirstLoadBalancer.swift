/*
 * Copyright 2024, gRPC Authors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package import GRPCCore
private import Synchronization

/// A load-balancer which has a single subchannel.
///
/// This load-balancer starts in an 'idle' state and begins connecting when a set of addresses is
/// provided to it with ``updateEndpoint(_:)``. Repeated calls to ``updateEndpoint(_:)`` will
/// update the subchannel gracefully: RPCs will continue to use the old subchannel until the new
/// subchannel becomes ready.
///
/// You must call ``close()`` on the load-balancer when it's no longer required. This will move
/// it to the ``ConnectivityState/shutdown`` state: existing RPCs may continue but all subsequent
/// calls to ``makeStream(descriptor:options:)`` will fail.
///
/// To use this load-balancer you must run it in a task and provide an event listener callback:
///
/// ```swift
/// await withDiscardingTaskGroup { group in
///   let pickFirst = PickFirstLoadBalancer(
///     // ... other parameters ...
///   ) { event, loadBalancerID in
///     switch event {
///     case .connectivityStateChanged(.ready):
///       // ...
///     default:
///       // ...
///     }
///   }
///
///   // Run the load-balancer
///   group.addTask { await pickFirst.run() }
///
///   // Update its endpoint.
///   let endpoint = Endpoint(
///     addresses: [
///       .ipv4(host: "127.0.0.1", port: 1001),
///       .ipv4(host: "127.0.0.1", port: 1002),
///       .ipv4(host: "127.0.0.1", port: 1003)
///     ]
///   )
///   pickFirst.updateEndpoint(endpoint)
/// }
/// ```
@available(gRPCSwiftNIOTransport 2.0, *)
package final class PickFirstLoadBalancer: Sendable {
  enum Input: Sendable, Hashable {
    /// Update the addresses used by the load balancer to the following endpoints.
    case updateEndpoint(Endpoint)
    /// Close the load balancer.
    case close
  }

  /// Events which can happen to the load balancer.
  private let eventListener: @Sendable (_ event: LoadBalancerEvent?, _ id: LoadBalancerID) -> Void

  /// Inputs which this load balancer should react to.
  private let input: (stream: AsyncStream<Input>, continuation: AsyncStream<Input>.Continuation)

  /// A connector, capable of creating connections.
  private let connector: any HTTP2Connector

  /// The percent-encoded server authority. If `nil`, a value will be computed based on the endpoint
  /// being connected to.
  private let authority: String?

  /// Connection backoff configuration.
  private let backoff: Backoff

  /// The default compression algorithm to use. Can be overridden on a per-call basis.
  private let defaultCompression: CompressionAlgorithm

  /// The set of enabled compression algorithms.
  private let enabledCompression: CompressionAlgorithmSet

  /// The state of the load-balancer.
  private let state: Mutex<State>

  /// The ID of this load balancer.
  internal let id: LoadBalancerID

  package init(
    connector: any HTTP2Connector,
    authority: String?,
    backoff: Backoff,
    defaultCompression: CompressionAlgorithm,
    enabledCompression: CompressionAlgorithmSet,
    eventListener: @Sendable @escaping (_ event: LoadBalancerEvent?, _ id: LoadBalancerID) -> Void
  ) {
    self.connector = connector
    self.authority = authority
    self.backoff = backoff
    self.defaultCompression = defaultCompression
    self.enabledCompression = enabledCompression
    self.id = LoadBalancerID()
    self.state = Mutex(State())

    self.eventListener = eventListener
    self.input = AsyncStream.makeStream(of: Input.self)
  }

  /// Runs the load balancer, returning when it has closed.
  ///
  /// Events are delivered to the event listener callback provided during initialization.
  package func run() async {
    // The load balancer starts in the idle state.
    self.eventListener(.connectivityStateChanged(.idle), self.id)

    await withDiscardingTaskGroup { group in
      for await input in self.input.stream {
        switch input {
        case .updateEndpoint(let endpoint):
          self.handleUpdateEndpoint(endpoint, in: &group)
        case .close:
          self.handleCloseInput()
        }
      }
    }

    if Task.isCancelled {
      // Finish the event stream as it's unlikely to have been finished by a regular code path.
      self.eventListener(nil, self.id)
    }
  }

  /// Update the addresses used by the load balancer.
  ///
  /// This may result in new subchannels being created and some subchannels being removed.
  package func updateEndpoint(_ endpoint: Endpoint) {
    self.input.continuation.yield(.updateEndpoint(endpoint))
  }

  /// Close the load balancer, and all subchannels it manages.
  package func close() {
    self.input.continuation.yield(.close)
  }

  /// Pick a ready subchannel from the load balancer.
  ///
  /// - Returns: A subchannel, or `nil` if there aren't any ready subchannels.
  package func pickSubchannel() -> Subchannel? {
    let onPickSubchannel = self.state.withLock { $0.pickSubchannel() }
    switch onPickSubchannel {
    case .picked(let subchannel):
      return subchannel
    case .notAvailable(let subchannel):
      subchannel?.connect()
      return nil
    }
  }
}

@available(gRPCSwiftNIOTransport 2.0, *)
extension PickFirstLoadBalancer {
  private func handleUpdateEndpoint(_ endpoint: Endpoint, in group: inout DiscardingTaskGroup) {
    if endpoint.addresses.isEmpty { return }

    let onUpdate = self.state.withLock { state in
      state.updateEndpoint(endpoint) { endpoint, id in
        Subchannel(
          endpoint: endpoint,
          id: id,
          connector: self.connector,
          authority: self.authority,
          backoff: self.backoff,
          defaultCompression: self.defaultCompression,
          enabledCompression: self.enabledCompression,
          eventListener: { self.handleSubchannelEvent($0, subchannelID: $1) }
        )
      }
    }

    switch onUpdate {
    case .connect(let newSubchannel, close: let oldSubchannel):
      self.runSubchannel(newSubchannel, in: &group)
      oldSubchannel?.shutDown()

    case .none:
      ()
    }
  }

  private func handleSubchannelEvent(_ event: Subchannel.Event?, subchannelID: SubchannelID) {
    switch event {
    case .connectivityStateChanged(let connectivityState):
      self.handleSubchannelConnectivityStateChange(connectivityState, id: subchannelID)
    case .goingAway:
      self.handleGoAway(id: subchannelID)
    case .requiresNameResolution:
      self.eventListener(.requiresNameResolution, self.id)
    case nil:
      ()
    }
  }

  private func runSubchannel(
    _ subchannel: Subchannel,
    in group: inout DiscardingTaskGroup
  ) {
    // Start running it and tell it to connect.
    subchannel.connect()
    group.addTask {
      await subchannel.run()
    }
  }

  private func handleSubchannelConnectivityStateChange(
    _ connectivityState: ConnectivityState,
    id: SubchannelID
  ) {
    let onUpdateState = self.state.withLock {
      $0.updateSubchannelConnectivityState(connectivityState, id: id)
    }

    switch onUpdateState {
    case .close(let subchannel):
      subchannel.shutDown()
    case .closeAndPublishStateChange(let subchannel, let connectivityState):
      subchannel.shutDown()
      self.eventListener(.connectivityStateChanged(connectivityState), self.id)
    case .publishStateChange(let connectivityState):
      self.eventListener(.connectivityStateChanged(connectivityState), self.id)
    case .closed:
      self.eventListener(nil, self.id)
      self.input.continuation.finish()
    case .none:
      ()
    }
  }

  private func handleGoAway(id: SubchannelID) {
    self.state.withLock { state in
      state.receivedGoAway(id: id)
    }
  }

  private func handleCloseInput() {
    let onClose = self.state.withLock { $0.close() }
    switch onClose {
    case .closeSubchannels(let subchannel1, let subchannel2):
      self.eventListener(.connectivityStateChanged(.shutdown), self.id)
      subchannel1.shutDown()
      subchannel2?.shutDown()

    case .closed:
      self.eventListener(.connectivityStateChanged(.shutdown), self.id)
      self.eventListener(nil, self.id)
      self.input.continuation.finish()

    case .none:
      ()
    }
  }
}

@available(gRPCSwiftNIOTransport 2.0, *)
extension PickFirstLoadBalancer {
  enum State: Sendable {
    case active(Active)
    case closing(Closing)
    case closed

    init() {
      self = .active(Active())
    }
  }
}

@available(gRPCSwiftNIOTransport 2.0, *)
extension PickFirstLoadBalancer.State {
  struct Active: Sendable {
    var endpoint: Endpoint?
    var connectivityState: ConnectivityState
    var current: Subchannel?
    var next: Subchannel?
    var parked: [SubchannelID: Subchannel]
    var isCurrentGoingAway: Bool

    init() {
      self.endpoint = nil
      self.connectivityState = .idle
      self.current = nil
      self.next = nil
      self.parked = [:]
      self.isCurrentGoingAway = false
    }
  }

  struct Closing: Sendable {
    var parked: [SubchannelID: Subchannel]

    init(from state: Active) {
      self.parked = state.parked
    }
  }
}

@available(gRPCSwiftNIOTransport 2.0, *)
extension PickFirstLoadBalancer.State.Active {
  mutating func updateEndpoint(
    _ endpoint: Endpoint,
    makeSubchannel: (_ endpoint: Endpoint, _ id: SubchannelID) -> Subchannel
  ) -> PickFirstLoadBalancer.State.OnUpdateEndpoint {
    if self.endpoint == endpoint { return .none }

    let onUpdateEndpoint: PickFirstLoadBalancer.State.OnUpdateEndpoint

    let id = SubchannelID()
    let newSubchannel = makeSubchannel(endpoint, id)

    switch (self.current, self.next) {
    case (.some(let current), .none):
      if self.connectivityState == .idle {
        // Current subchannel is idle and we have a new endpoint, move straight to the new
        // subchannel.
        self.isCurrentGoingAway = false
        self.current = newSubchannel
        self.parked[current.id] = current
        onUpdateEndpoint = .connect(newSubchannel, close: current)
      } else {
        // Current subchannel is in a non-idle state, set it as the next subchannel and promote
        // it when it becomes ready.
        self.next = newSubchannel
        onUpdateEndpoint = .connect(newSubchannel, close: nil)
      }

    case (.some, .some(let next)):
      // Current and next subchannel exist. Replace the next subchannel.
      self.next = newSubchannel
      self.parked[next.id] = next
      onUpdateEndpoint = .connect(newSubchannel, close: next)

    case (.none, .none):
      self.isCurrentGoingAway = false
      self.current = newSubchannel
      onUpdateEndpoint = .connect(newSubchannel, close: nil)

    case (.none, .some(let next)):
      self.isCurrentGoingAway = false
      self.current = newSubchannel
      self.next = nil
      self.parked[next.id] = next
      onUpdateEndpoint = .connect(newSubchannel, close: next)
    }

    return onUpdateEndpoint
  }

  mutating func updateSubchannelConnectivityState(
    _ connectivityState: ConnectivityState,
    id: SubchannelID
  ) -> (PickFirstLoadBalancer.State.OnConnectivityStateUpdate, PickFirstLoadBalancer.State) {
    let onUpdate: PickFirstLoadBalancer.State.OnConnectivityStateUpdate

    if let current = self.current, current.id == id {
      if connectivityState == self.connectivityState {
        onUpdate = .none
      } else {
        // If the subchannel was going away and became idle then it went away.
        if connectivityState == .idle {
          self.isCurrentGoingAway = false
        }
        self.connectivityState = connectivityState
        onUpdate = .publishStateChange(connectivityState)
      }
    } else if let next = self.next, next.id == id {
      // if it becomes ready then promote it
      switch connectivityState {
      case .ready:
        if self.connectivityState != connectivityState {
          self.connectivityState = connectivityState

          if let current = self.current {
            onUpdate = .closeAndPublishStateChange(current, connectivityState)
          } else {
            onUpdate = .publishStateChange(connectivityState)
          }
        } else {
          // No state change to publish, just roll over.
          onUpdate = self.current.map { .close($0) } ?? .none
        }
        self.current = next
        self.next = nil
        self.isCurrentGoingAway = false

      case .idle, .connecting, .transientFailure, .shutdown:
        onUpdate = .none
      }

    } else {
      switch connectivityState {
      case .idle:
        if let subchannel = self.parked[id] {
          onUpdate = .close(subchannel)
        } else {
          onUpdate = .none
        }

      case .shutdown:
        self.parked.removeValue(forKey: id)
        onUpdate = .none

      case .connecting, .ready, .transientFailure:
        onUpdate = .none
      }
    }

    return (onUpdate, .active(self))
  }

  mutating func receivedGoAway(id: SubchannelID) {
    if let current = self.current, current.id == id {
      // When receiving a GOAWAY the subchannel will ask for an address to be re-resolved and the
      // connection will eventually become idle. At this point we wait: the connection remains
      // in its current state.
      self.isCurrentGoingAway = true
    } else if let next = self.next, next.id == id {
      // The next connection is going away, park it.
      // connection.
      self.next = nil
      self.parked[next.id] = next
    }
  }

  mutating func close() -> (PickFirstLoadBalancer.State.OnClose, PickFirstLoadBalancer.State) {
    let onClose: PickFirstLoadBalancer.State.OnClose
    let nextState: PickFirstLoadBalancer.State

    if let current = self.current {
      self.parked[current.id] = current
      if let next = self.next {
        self.parked[next.id] = next
        onClose = .closeSubchannels(current, next)
      } else {
        onClose = .closeSubchannels(current, nil)
      }
      nextState = .closing(PickFirstLoadBalancer.State.Closing(from: self))
    } else {
      onClose = .closed
      nextState = .closed
    }

    return (onClose, nextState)
  }

  func pickSubchannel() -> PickFirstLoadBalancer.State.OnPickSubchannel {
    let onPick: PickFirstLoadBalancer.State.OnPickSubchannel

    if let current = self.current, !self.isCurrentGoingAway {
      switch self.connectivityState {
      case .idle:
        onPick = .notAvailable(current)
      case .ready:
        onPick = .picked(current)
      case .connecting, .transientFailure, .shutdown:
        onPick = .notAvailable(nil)
      }
    } else {
      onPick = .notAvailable(nil)
    }

    return onPick
  }
}

@available(gRPCSwiftNIOTransport 2.0, *)
extension PickFirstLoadBalancer.State.Closing {
  mutating func updateSubchannelConnectivityState(
    _ connectivityState: ConnectivityState,
    id: SubchannelID
  ) -> (PickFirstLoadBalancer.State.OnConnectivityStateUpdate, PickFirstLoadBalancer.State) {
    let onUpdate: PickFirstLoadBalancer.State.OnConnectivityStateUpdate
    let nextState: PickFirstLoadBalancer.State

    switch connectivityState {
    case .idle:
      if let subchannel = self.parked[id] {
        onUpdate = .close(subchannel)
      } else {
        onUpdate = .none
      }
      nextState = .closing(self)

    case .shutdown:
      if self.parked.removeValue(forKey: id) != nil {
        if self.parked.isEmpty {
          onUpdate = .closed
          nextState = .closed
        } else {
          onUpdate = .none
          nextState = .closing(self)
        }
      } else {
        onUpdate = .none
        nextState = .closing(self)
      }

    case .connecting, .ready, .transientFailure:
      onUpdate = .none
      nextState = .closing(self)
    }

    return (onUpdate, nextState)
  }
}

@available(gRPCSwiftNIOTransport 2.0, *)
extension PickFirstLoadBalancer.State {
  enum OnUpdateEndpoint {
    case connect(Subchannel, close: Subchannel?)
    case none
  }

  mutating func updateEndpoint(
    _ endpoint: Endpoint,
    makeSubchannel: (_ endpoint: Endpoint, _ id: SubchannelID) -> Subchannel
  ) -> OnUpdateEndpoint {
    let onUpdateEndpoint: OnUpdateEndpoint

    switch self {
    case .active(var state):
      onUpdateEndpoint = state.updateEndpoint(endpoint) { endpoint, id in
        makeSubchannel(endpoint, id)
      }
      self = .active(state)

    case .closing, .closed:
      onUpdateEndpoint = .none
    }

    return onUpdateEndpoint
  }

  enum OnConnectivityStateUpdate {
    case closeAndPublishStateChange(Subchannel, ConnectivityState)
    case publishStateChange(ConnectivityState)
    case close(Subchannel)
    case closed
    case none
  }

  mutating func updateSubchannelConnectivityState(
    _ connectivityState: ConnectivityState,
    id: SubchannelID
  ) -> OnConnectivityStateUpdate {
    let onUpdateState: OnConnectivityStateUpdate

    switch self {
    case .active(var state):
      (onUpdateState, self) = state.updateSubchannelConnectivityState(connectivityState, id: id)
    case .closing(var state):
      (onUpdateState, self) = state.updateSubchannelConnectivityState(connectivityState, id: id)
    case .closed:
      onUpdateState = .none
    }

    return onUpdateState
  }

  mutating func receivedGoAway(id: SubchannelID) {
    switch self {
    case .active(var state):
      state.receivedGoAway(id: id)
      self = .active(state)
    case .closing, .closed:
      ()
    }
  }

  enum OnClose {
    case closeSubchannels(Subchannel, Subchannel?)
    case closed
    case none
  }

  mutating func close() -> OnClose {
    let onClose: OnClose

    switch self {
    case .active(var state):
      (onClose, self) = state.close()
    case .closing, .closed:
      onClose = .none
    }

    return onClose
  }

  enum OnPickSubchannel {
    case picked(Subchannel)
    case notAvailable(Subchannel?)
  }

  func pickSubchannel() -> OnPickSubchannel {
    switch self {
    case .active(let state):
      return state.pickSubchannel()
    case .closing, .closed:
      return .notAvailable(nil)
    }
  }
}
