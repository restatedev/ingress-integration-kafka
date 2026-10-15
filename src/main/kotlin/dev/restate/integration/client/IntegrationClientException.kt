// Copyright (c) 2026 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Kafka ingress integration,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/ingress-integration-kafka/blob/main/LICENSE
package dev.restate.integration.client

class IntegrationClientException(
    val kind: Kind,
    message: String? = null,
    cause: Throwable? = null,
) : RuntimeException(message ?: kind.name, cause) {

  enum class Kind {
    UNKNOWN,
    SHUTTING_DOWN,
    GO_AWAY,
    NOT_FOUND,
    BAD_REQUEST,
  }

  fun isRetryable(): Boolean = kind != Kind.BAD_REQUEST
}
