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

public import NIOCore
internal import NIOExtras

/// A factory to produce `NIOAsyncChannel`s to listen for new HTTP/2 connections.
///
/// - SeeAlso: ``CommonHTTP2ServerTransport``
@available(gRPCSwiftNIOTransport 2.5, *)
extension HTTP2ServerTransport {
  public struct ListenerParameters: Sendable {
    var quiescingHelper: ServerQuiescingHelper

    public func configureListener(
      channel: any Channel,
      debuggingCallbacks: HTTP2ServerTransport.Config.ChannelDebuggingCallbacks
    ) -> EventLoopFuture<Void> {
      let configured = channel.eventLoop.makeCompletedFuture {
        let quiescingHandler = self.quiescingHelper.makeServerChannelHandler(
          channel: channel
        )
        try channel.pipeline.syncOperations.addHandler(quiescingHandler)
      }

      return configured.runInitializerIfSet(debuggingCallbacks.onBindTCPListener, on: channel)
    }
  }

  public struct ConnectionParameters: Sendable {
    public func configureConnection(
      channel: any Channel,
      compressionConfig: HTTP2ServerTransport.Config.Compression,
      connectionConfig: HTTP2ServerTransport.Config.Connection,
      http2Config: HTTP2ServerTransport.Config.HTTP2,
      rpcConfig: HTTP2ServerTransport.Config.RPC,
      debuggingCallbacks: HTTP2ServerTransport.Config.ChannelDebuggingCallbacks,
      usesTLS: Bool,
      requireALPN: Bool
    ) -> EventLoopFuture<ConnectionChannel> {
      self.configureConnection(
        channel: channel,
        sslHandler: nil,
        compressionConfig: compressionConfig,
        connectionConfig: connectionConfig,
        http2Config: http2Config,
        rpcConfig: rpcConfig,
        debuggingCallbacks: debuggingCallbacks,
        usesTLS: usesTLS,
        requireALPN: requireALPN
      )
    }

    package func configureConnection(
      channel: any Channel,
      sslHandler: (any ChannelHandler)?,
      compressionConfig: HTTP2ServerTransport.Config.Compression,
      connectionConfig: HTTP2ServerTransport.Config.Connection,
      http2Config: HTTP2ServerTransport.Config.HTTP2,
      rpcConfig: HTTP2ServerTransport.Config.RPC,
      debuggingCallbacks: HTTP2ServerTransport.Config.ChannelDebuggingCallbacks,
      usesTLS: Bool,
      requireALPN: Bool
    ) -> EventLoopFuture<ConnectionChannel> {
      let configured = channel.eventLoop.makeCompletedFuture {
        if let sslHandler {
          try channel.pipeline.syncOperations.addHandler(sslHandler)
        }

        let (connection, mux) = try channel.pipeline.syncOperations.configureGRPCServerPipeline(
          channel: channel,
          compressionConfig: compressionConfig,
          connectionConfig: connectionConfig,
          http2Config: http2Config,
          rpcConfig: rpcConfig,
          debugConfig: debuggingCallbacks,
          requireALPN: requireALPN,
          scheme: usesTLS ? .https : .http
        )

        return ConnectionChannel(connection: connection, multiplexer: mux)
      }

      return configured.runInitializerIfSet(debuggingCallbacks.onAcceptTCPConnection, on: channel)
    }
  }

  public struct ConnectionChannel: Sendable {
    let connection: ChannelPipeline.SynchronousOperations.HTTP2ConnectionChannel
    let multiplexer: ChannelPipeline.SynchronousOperations.HTTP2StreamMultiplexer
  }

  package protocol ListenerFactory: Sendable {
    func makeListeningChannel(
      listenerParameters: ListenerParameters,
      connectionParameters: ConnectionParameters
    ) async throws -> NIOAsyncChannel<ConnectionChannel, Never>
  }
}
