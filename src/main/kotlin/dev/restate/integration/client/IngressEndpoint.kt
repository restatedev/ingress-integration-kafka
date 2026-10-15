// Copyright (c) 2026 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Kafka ingress integration,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/ingress-integration-kafka/blob/main/LICENSE
package dev.restate.integration.client

/** Where to reach the Restate ingestion gRPC endpoint. */
data class IngressEndpoint(
    val host: String,
    val port: Int,
    val tls: Boolean,
    val authToken: String? = null,
) {
  override fun toString(): String {
    return "${if (tls) "https" else "http"}://${host}:${port}"
  }
}
