/*
 * Copyright 2025, gRPC Authors All rights reserved.
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

import GRPCCore
import GRPCNIOTransportHTTP2

enum TransportKind: CaseIterable, Hashable, Sendable {
  case posix
  #if canImport(Network)
  case transportServices
  #endif
  case tunnel

  static var clients: [Self] {
    Self.allCases
  }

  static var clientsWithTLS: [Self] {
    Self.allCases.filter { $0 != .tunnel }
  }

  static var servers: [Self] {
    Self.allCases.filter { $0 != .tunnel }
  }

  static var serversWithTLS: [Self] {
    Self.allCases.filter { $0 != .tunnel }
  }
}

enum NIOClientTransport: ClientTransport {
  case posix(HTTP2ClientTransport.Posix)
  #if canImport(Network)
  case transportServices(HTTP2ClientTransport.TransportServices)
  #endif
  case tunnel(GRPCTunnelClientTransport)

  init(_ transport: HTTP2ClientTransport.Posix) {
    self = .posix(transport)
  }

  #if canImport(Network)
  init(_ transport: HTTP2ClientTransport.TransportServices) {
    self = .transportServices(transport)
  }
  #endif

  init(_ transport: GRPCTunnelClientTransport) {
    self = .tunnel(transport)
  }

  typealias Bytes = GRPCNIOTransportBytes

  var retryThrottle: GRPCCore.RetryThrottle? {
    switch self {
    case .posix(let transport):
      return transport.retryThrottle
    #if canImport(Network)
    case .transportServices(let transport):
      return transport.retryThrottle
    #endif
    case .tunnel(let transport):
      return transport.retryThrottle
    }
  }

  func connect() async throws {
    switch self {
    case .posix(let transport):
      try await transport.connect()
    #if canImport(Network)
    case .transportServices(let transport):
      try await transport.connect()
    #endif
    case .tunnel(let transport):
      try await transport.connect()
    }
  }

  func beginGracefulShutdown() {
    switch self {
    case .posix(let transport):
      transport.beginGracefulShutdown()
    #if canImport(Network)
    case .transportServices(let transport):
      transport.beginGracefulShutdown()
    #endif
    case .tunnel(let transport):
      transport.beginGracefulShutdown()
    }
  }

  func withStream<T: Sendable>(
    descriptor: MethodDescriptor,
    options: CallOptions,
    _ closure: (_ stream: RPCStream<Inbound, Outbound>, _ context: ClientContext) async throws -> T
  ) async throws -> T {
    switch self {
    case .posix(let transport):
      return try await transport.withStream(descriptor: descriptor, options: options, closure)
    #if canImport(Network)
    case .transportServices(let transport):
      return try await transport.withStream(descriptor: descriptor, options: options, closure)
    #endif
    case .tunnel(let transport):
      return try await transport.withStream(descriptor: descriptor, options: options, closure)
    }
  }

  func config(forMethod descriptor: GRPCCore.MethodDescriptor) -> GRPCCore.MethodConfig? {
    switch self {
    case .posix(let transport):
      return transport.config(forMethod: descriptor)
    #if canImport(Network)
    case .transportServices(let transport):
      return transport.config(forMethod: descriptor)
    #endif
    case .tunnel(let transport):
      return transport.config(forMethod: descriptor)
    }
  }

}

enum NIOServerTransport: ServerTransport, ListeningServerTransport {
  case posix(HTTP2ServerTransport.Posix)
  #if canImport(Network)
  case transportServices(HTTP2ServerTransport.TransportServices)
  #endif

  init(_ transport: HTTP2ServerTransport.Posix) {
    self = .posix(transport)
  }

  #if canImport(Network)
  init(_ transport: HTTP2ServerTransport.TransportServices) {
    self = .transportServices(transport)
  }
  #endif

  typealias Bytes = GRPCNIOTransportBytes

  var listeningAddress: GRPCNIOTransportCore.SocketAddress {
    get async throws {
      switch self {
      case .posix(let transport):
        try await transport.listeningAddress
      #if canImport(Network)
      case .transportServices(let transport):
        try await transport.listeningAddress
      #endif
      }
    }
  }

  func listen(
    streamHandler: @escaping @Sendable (
      _ stream: RPCStream<Inbound, Outbound>,
      _ context: ServerContext
    ) async -> Void
  ) async throws {
    switch self {
    case .posix(let transport):
      try await transport.listen(streamHandler: streamHandler)
    #if canImport(Network)
    case .transportServices(let transport):
      try await transport.listen(streamHandler: streamHandler)
    #endif
    }
  }

  func beginGracefulShutdown() {
    switch self {
    case .posix(let transport):
      transport.beginGracefulShutdown()
    #if canImport(Network)
    case .transportServices(let transport):
      transport.beginGracefulShutdown()
    #endif
    }
  }
}
