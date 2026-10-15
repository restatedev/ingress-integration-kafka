// Copyright (c) 2026 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Kafka ingress integration,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/ingress-integration-kafka/blob/main/LICENSE
package dev.restate.integration.client

import io.vertx.core.Vertx

/** Opens `Ingest` streams against a Restate ingress endpoint. */
interface IntegrationClient {

  suspend fun open(
      producerId: String,
      listener: InvocationStream.Listener,
      initialStreamDefaults: StreamDefaults,
  ): InvocationStream

  suspend fun close()

  companion object {
    /** Build a client with a connection wired for [endpoints]'s transport. */
    fun connect(
        vertx: Vertx,
        endpoints: List<IngressEndpoint>,
    ): IntegrationClient = IntegrationClientImpl.connect(vertx, endpoints)
  }
}
