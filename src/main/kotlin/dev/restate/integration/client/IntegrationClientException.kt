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

  fun isRetryable(): Boolean = kind != Kind.NOT_FOUND && kind != Kind.BAD_REQUEST
}
