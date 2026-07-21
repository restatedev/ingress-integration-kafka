import com.google.protobuf.gradle.id

plugins {
  kotlin("jvm") version "2.3.21"
  id("com.google.protobuf") version "0.10.0"
  id("com.google.cloud.tools.jib") version "3.5.3"
  id("com.diffplug.spotless") version "8.8.0"
  application
}

group = "dev.restate"

version = "1.0-SNAPSHOT"

repositories { mavenCentral() }

val vertxVersion = "5.1.5"
val protobufVersion = "4.29.3"

// Silence the JDK 24+ "sun.misc.Unsafe::... has been called" warnings: protobuf-java still uses
// Unsafe for fast (de)serialization. `allow` keeps it working without the per-call-site warning.
val runtimeJvmArgs = listOf("--sun-misc-unsafe-memory-access=allow")

dependencies {
  implementation(platform("io.vertx:vertx-stack-depchain:$vertxVersion"))

  implementation("io.vertx:vertx-core")
  implementation("io.vertx:vertx-launcher-application")
  implementation("io.vertx:vertx-grpc-client")
  implementation("io.vertx:vertx-kafka-client")
  implementation("io.vertx:vertx-lang-kotlin")
  implementation("io.vertx:vertx-lang-kotlin-coroutines")

  // Metrics: Vert.x Micrometer integration (event loops, HTTP/2 client to Restate, embedded
  // Prometheus scrape server) backed by a Prometheus registry. micrometer-registry-prometheus
  // transitively brings micrometer-core, which carries the KafkaClientMetrics + JVM binders.
  // vertx-micrometer-metrics is BOM-managed; the registry is not, so pin it to the same Micrometer
  // version Vert.x 5.1.5 resolves (io.micrometer:micrometer-core:1.16.6) to keep them aligned.
  implementation("io.vertx:vertx-micrometer-metrics")
  implementation("io.micrometer:micrometer-registry-prometheus:1.16.6")
  // ProducerSession uses coroutines (launch/CoroutineScope/delay) directly.
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

  // gRPC message (de)serialization + generated message types.
  implementation("com.google.protobuf:protobuf-java:$protobufVersion")

  // Jackson for JSON-based record mappers (JsonDynamicTargetRecordMapper): JsonNode + JsonPointer.
  // Vert.x ships jackson-core but leaves databind optional, so add it (aligned to Vert.x's 2.21.x).
  implementation("com.fasterxml.jackson.core:jackson-databind:2.21.5")

  // The Vert.x gRPC codegen also emits a server stub (IngestionSvcGrpcService) that imports
  // io.vertx.grpc.server.*. We only run the client at runtime, so keep the server API
  // compile-only here (the generated service class is never loaded in production) and pull it
  // in fully for tests, where the in-process fake IngestionSvc server uses it.
  compileOnly("io.vertx:vertx-grpc-server")

  // Logging: our code and Vert.x use the Log4j2 API directly. Kafka hard-depends on SLF4J, so
  // its logs are bridged into Log4j2 too.
  implementation(platform("org.apache.logging.log4j:log4j-bom:2.26.1"))
  implementation("org.apache.logging.log4j:log4j-api")
  runtimeOnly("org.apache.logging.log4j:log4j-core")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl") // Kafka SLF4J -> Log4j2

  // Test stack: JUnit 6 (Jupiter) + AssertJ. Vertx lifecycle is driven manually via coroutines.
  testImplementation(platform("org.junit:junit-bom:6.1.2"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation("org.assertj:assertj-core:3.27.7")
  testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
  testImplementation("org.testcontainers:testcontainers:2.0.4")
  testImplementation("org.testcontainers:testcontainers-kafka:2.0.4")
}

kotlin { jvmToolchain(25) }

spotless {
  kotlin {
    target("src/**/*.kt")
    ktfmt()
  }
  kotlinGradle {
    target("*.gradle.kts")
    ktfmt()
  }
}

application {
  mainClass.set("dev.restate.integration.kafka.MainKt")
  // Applies to `./gradlew run` and the installDist launch script (Jib sets its own flags below).
  applicationDefaultJvmArgs = runtimeJvmArgs
}

protobuf {
  protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" }
  plugins { id("vertx") { artifact = "io.vertx:vertx-grpc-protoc-plugin2:$vertxVersion" } }
  generateProtoTasks {
    // Register the Vert.x gRPC generator (emits both the *Client and *Service stubs
    // alongside protoc's built-in Java message classes).
    all().forEach { it.plugins { id("vertx") } }
  }
}

tasks.test {
  useJUnitPlatform()
  // Ryuk (Testcontainers' reaper) is unreliable on rootless podman; the e2e test cleans up its own
  // containers in a finally block, so disabling it keeps `./gradlew test` working there.
  environment("TESTCONTAINERS_RYUK_DISABLED", "true")
}

// Container image built by Jib (no Docker daemon / Dockerfile needed).
//   ./gradlew jibBuildTar   -> build to build/jib-image.tar (offline-verifiable)
//   ./gradlew jibDockerBuild-> build into the local Docker/Podman daemon
//   ./gradlew jib           -> build and push to the registry
jib {
  from {
    image = "eclipse-temurin:25-jre"
    // For a multi-arch release push, add an arm64 platform here (tar builds must stay single-arch).
    platforms {
      platform {
        architecture = "amd64"
        os = "linux"
      }
    }
  }
  to {
    image = "ghcr.io/restatedev/ingress-integration-kafka"
    tags = setOf("latest", version.toString())
  }
  container {
    mainClass = "dev.restate.integration.kafka.MainKt"
    jvmFlags = runtimeJvmArgs
    // Prometheus scrape endpoint (see RESTATE_METRICS_PORT); informational, does not publish it.
    ports = listOf("9464")
  }
}
