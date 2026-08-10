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
