import com.google.protobuf.gradle.id

plugins {
  kotlin("jvm") version "2.3.21"
  id("com.google.protobuf") version "0.10.0"
  id("com.google.cloud.tools.jib") version "3.5.3"
  application
}

group = "dev.restate"

version = "1.0-SNAPSHOT"

repositories { mavenCentral() }

val vertxVersion = "5.1.5"
// Must match the protobuf-java version that Vert.x 5.1.5 depends on (see vertx-grpc-aggregator).
val protobufVersion = "4.29.3"

dependencies {
  implementation(platform("io.vertx:vertx-stack-depchain:$vertxVersion"))

  implementation("io.vertx:vertx-core")
  implementation("io.vertx:vertx-launcher-application")
  implementation("io.vertx:vertx-grpc-client")
  implementation("io.vertx:vertx-kafka-client")
  implementation("io.vertx:vertx-lang-kotlin")
  implementation("io.vertx:vertx-lang-kotlin-coroutines")

  // gRPC message (de)serialization + generated message types.
  implementation("com.google.protobuf:protobuf-java:$protobufVersion")

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
  // Used to stand up an in-process fake IngestionSvc server in tests.
  testImplementation("io.vertx:vertx-grpc-server")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin { jvmToolchain(25) }

application { mainClass.set("dev.restate.integration.kafka.MainKt") }

protobuf {
  protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" }
  plugins { id("vertx") { artifact = "io.vertx:vertx-grpc-protoc-plugin2:$vertxVersion" } }
  generateProtoTasks {
    // Register the Vert.x gRPC generator (emits both the *Client and *Service stubs
    // alongside protoc's built-in Java message classes).
    all().forEach { it.plugins { id("vertx") } }
  }
}

tasks.test { useJUnitPlatform() }

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
  container { mainClass = "dev.restate.integration.kafka.MainKt" }
}
