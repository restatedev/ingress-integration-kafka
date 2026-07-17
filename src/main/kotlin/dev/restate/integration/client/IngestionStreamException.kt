package dev.restate.integration.client

class IngestionStreamException(val kind: Kind, message: String? = null, cause: Throwable? = null) :
    RuntimeException(message ?: kind.name, cause) {

  enum class Kind {
    UNKNOWN,
    SHUTTING_DOWN,
    UNKNOWN_SERVICE,
    UNKNOWN_HANDLER,
  }

  fun isRetryable(): Boolean {
    return kind == Kind.SHUTTING_DOWN || kind == Kind.UNKNOWN
  }
}
