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

extension GRPCTunnelClientTransport {
  public struct Config: Sendable {
    public var http2: HTTP2ClientTransport.Config.HTTP2
    public var backoff: HTTP2ClientTransport.Config.Backoff
    public var connection: HTTP2ClientTransport.Config.Connection
    public var compression: HTTP2ClientTransport.Config.Compression

    public init(
      http2: HTTP2ClientTransport.Config.HTTP2,
      backoff: HTTP2ClientTransport.Config.Backoff,
      connection: HTTP2ClientTransport.Config.Connection,
      compression: HTTP2ClientTransport.Config.Compression
    ) {
      self.http2 = http2
      self.backoff = backoff
      self.connection = connection
      self.compression = compression
    }

    public static var defaults: Self {
      Self.defaults { _ in }
    }

    public static func defaults(_ configure: (inout Self) -> Void) -> Self {
      var config = Self(
        http2: .defaults,
        backoff: .defaults,
        connection: .defaults,
        compression: .defaults
      )
      configure(&config)
      return config
    }
  }
}

extension GRPCChannel.Config {
  init(_ config: GRPCTunnelClientTransport.Config) {
    self.init(
      http2: config.http2,
      backoff: config.backoff,
      connection: config.connection,
      compression: config.compression
    )
  }
}
