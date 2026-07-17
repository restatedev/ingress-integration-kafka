// A minimal Restate SDK service used only by the Kafka->Restate end-to-end integration test as the
// invocation target. Built into a local image via `./gradlew :e2e-greeter:jibDockerBuild`.
//
// Isolated in its own module on purpose: the Restate Java SDK is built on Vert.x 4.5, which would
// clash with the main app's Vert.x 5 if it were on the same classpath.
plugins {
  java
  application
  id("com.google.cloud.tools.jib") version "3.5.3"
}

repositories { mavenCentral() }

// The Restate SDK 2.9.3 targets JVM 25.
java { toolchain { languageVersion = JavaLanguageVersion.of(25) } }

dependencies {
  implementation("dev.restate:sdk-api:2.9.3")
  implementation("dev.restate:sdk-http-vertx:2.9.3")
  // The codegen emits a typed client (GreeterClient) that references dev.restate.client.
  implementation("dev.restate:client:2.9.3")
  annotationProcessor("dev.restate:sdk-api-gen:2.9.3")
}

application { mainClass = "dev.restate.integration.e2egreeter.Main" }

jib {
  from { image = "eclipse-temurin:25-jre" }
  to { image = "restate-e2e-greeter:test" }
  container {
    mainClass = "dev.restate.integration.e2egreeter.Main"
    ports = listOf("9080")
  }
}
