import com.github.jk1.license.LicenseReportExtension
import com.github.jk1.license.ProjectData
import com.github.jk1.license.filter.LicenseBundleNormalizer
import com.github.jk1.license.render.InventoryHtmlReportRenderer
import com.github.jk1.license.render.ReportRenderer
import com.google.protobuf.gradle.id
import java.io.File
import java.util.concurrent.TimeUnit
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
  kotlin("jvm") version "2.3.21"
  id("com.google.cloud.tools.jib") version "3.5.3"
  id("com.diffplug.spotless") version "8.8.0"
  id("com.github.jk1.dependency-license-report") version "3.1.4"
  application
}

group = "dev.restate"

// Overridable from CI (e.g. `-Pversion=1.2.3` on a release tag) so the generated Version /
// integration string and image tags reflect the released version; defaults to a snapshot locally.
version =
    (findProperty("version")?.toString()?.takeUnless { it.isBlank() || it == "unspecified" })
        ?: "0.1-SNAPSHOT"

// mavenLocal() resolves the dev.restate:integration-client SNAPSHOT published from ../sdk-java
// (`./gradlew :integration-client:publishToMavenLocal`) until it ships to Maven Central.
repositories {
  mavenLocal()
  mavenCentral()
}

// Silence the JDK 24+ "sun.misc.Unsafe::... has been called" warnings: protobuf-java still uses
// Unsafe for fast (de)serialization. `allow` keeps it working without the per-call-site warning.
val runtimeJvmArgs = listOf("--sun-misc-unsafe-memory-access=allow")

dependencies {
  implementation(platform("io.vertx:vertx-stack-depchain:5.2.1"))

  implementation("io.vertx:vertx-core")
  implementation("io.vertx:vertx-launcher-application")
  // vertx-grpcio-client adapts a Vert.x gRPC client into an io.grpc.Channel (GrpcIoClientChannel),
  // which we feed to the sdk-java ingestion client so the transport runs on the Vert.x event loop.
  implementation("io.vertx:vertx-grpcio-client")
  implementation("io.vertx:vertx-kafka-client")
  implementation("io.vertx:vertx-lang-kotlin")
  implementation("io.vertx:vertx-lang-kotlin-coroutines")

  // The Restate ingestion client (ExactlyOnceProducer + IntegrationClient). We supply our own
  // Vert.x-backed io.grpc.Channel, so exclude the sdk's default grpc-netty-shaded transport.
  implementation("dev.restate:integration-client:2.10.0") {
    exclude(group = "io.grpc", module = "grpc-netty-shaded")
  }

  // Metrics: Vert.x Micrometer integration (event loops, HTTP/2 client to Restate, embedded
  // Prometheus scrape server) backed by a Prometheus registry. micrometer-registry-prometheus
  // transitively brings micrometer-core, which carries the KafkaClientMetrics + JVM binders.
  // vertx-micrometer-metrics is BOM-managed; the registry is not, so pin it to the same Micrometer
  // version Vert.x 5.1.5 resolves (io.micrometer:micrometer-core:1.16.6) to keep them aligned.
  implementation("io.vertx:vertx-micrometer-metrics")
  implementation("io.micrometer:micrometer-registry-prometheus:1.16.6")
  // PartitionSession uses coroutines (launch/CoroutineScope/delay) directly.
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

  // Jackson for JSON-based record mappers (JsonDynamicTargetRecordMapper): JsonNode + JsonPointer.
  // Vert.x ships jackson-core but leaves databind optional, so add it (aligned to Vert.x's 2.21.x).
  implementation("com.fasterxml.jackson.core:jackson-databind:2.21.7")

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
    licenseHeaderFile("$rootDir/config/license-header")
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

tasks.test {
  useJUnitPlatform()
  // Ryuk (Testcontainers' reaper) is unreliable on rootless podman; the e2e test cleans up its own
  // containers in a finally block, so disabling it keeps `./gradlew test` working there.
  environment("TESTCONTAINERS_RYUK_DISABLED", "true")
}

// Generate a Kotlin `Version` object at build time (mirrors the mechanism in ../sdk-java
// sdk-common/build.gradle.kts). VERSION comes from the Gradle `version`, GIT_HASH from git.
val generatedVersionDir = layout.buildDirectory.dir("version")

generatedVersionDir.get().asFile.mkdirs()

sourceSets { main { kotlin.srcDir(generatedVersionDir) } }

// From https://discuss.kotlinlang.org/t/use-git-hash-as-version-number-in-build-gradle-kts/19818/4
fun String.runCommand(
    workingDir: File = File("."),
    timeoutAmount: Long = 5,
    timeoutUnit: TimeUnit = TimeUnit.SECONDS,
): String =
    ProcessBuilder(split("\\s(?=(?:[^'\"`]*(['\"`])[^'\"`]*\\1)*[^'\"`]*$)".toRegex()))
        .directory(workingDir)
        .redirectOutput(ProcessBuilder.Redirect.PIPE)
        .redirectError(ProcessBuilder.Redirect.PIPE)
        .start()
        .apply { waitFor(timeoutAmount, timeoutUnit) }
        .run {
          val error = errorStream.bufferedReader().readText().trim()
          if (error.isNotEmpty()) {
            throw IllegalStateException(error)
          }
          inputStream.bufferedReader().readText().trim()
        }

val generateVersionClass =
    tasks.register("generateVersionClass") {
      dependsOn(project.tasks.processResources)
      outputs.dir(generatedVersionDir)

      doFirst {
        val gitHash = "git rev-parse --short=8 HEAD".runCommand(workingDir = rootDir)
        val containingDir = generatedVersionDir.get().dir("dev/restate/integration/version").asFile
        assert(containingDir.exists() || containingDir.mkdirs())

        file("$containingDir/Version.kt")
            .writeText(
                """
      package dev.restate.integration.version

      /** Generated at build time by the `generateVersionClass` Gradle task. Do not edit. */
      object Version {
        const val VERSION: String = "$version"
        const val GIT_HASH: String = "$gitHash"
        // Integration identifier for the ingestion protocol Start frame: `name/version`.
        const val INTEGRATION: String = "kafka/" + VERSION + "_" + GIT_HASH
      }
      """
                    .trimIndent()
            )

        check(
            file("${projectDir}/build/version/dev/restate/integration/version/Version.kt").exists()
        ) {
          "${projectDir}/build/version/dev/restate/integration/version/Version.kt doesn't exist?!"
        }
      }
    }

tasks {
  withType<JavaCompile>().configureEach { dependsOn(generateVersionClass) }
  withType<KotlinCompile>().configureEach { dependsOn(generateVersionClass) }
  withType<org.gradle.jvm.tasks.Jar>().configureEach { dependsOn(generateVersionClass) }
}

// Third-party licenses of what ships in the image (the runtime classpath). `checkLicense` (part of
// `check`) fails the build when a dependency has no license listed in config/allowed-licenses.json:
// review the new license before extending that list. `generateLicenseReport` renders the NOTICE
// shipped in the image plus an HTML inventory, under build/reports/dependency-license/.
licenseReport {
  configurations = arrayOf("runtimeClasspath")
  excludeBoms = true
  // Platforms that excludeBoms does not catch.
  excludes = arrayOf("io.vertx:vertx-stack-depchain", "com.google.guava:guava-parent")
  filters = arrayOf(LicenseBundleNormalizer())
  allowedLicensesFile = file("config/allowed-licenses.json")
  renderers = arrayOf(NoticeReportRenderer(), InventoryHtmlReportRenderer())
}

tasks.check { dependsOn(tasks.checkLicense) }

// NoticeReportRenderer lives in this script, which Gradle doesn't track as an input of the plugin's
// task: rerun the report whenever the script changes, so the NOTICE is never stale.
tasks.generateLicenseReport { inputs.file(buildFile) }

/**
 * Renders the `NOTICE` file shipped in the image: our copyright, every bundled third-party
 * component with the license(s) its POM declares, and the NOTICE files embedded in those components
 * reproduced verbatim (Apache License 2.0, section 4(d)), identical ones listed once.
 */
class NoticeReportRenderer : ReportRenderer {
  override fun render(data: ProjectData) {
    val outputDir =
        File(
            data.project.extensions.getByType(LicenseReportExtension::class.java).absoluteOutputDir
        )
    val modules = data.allDependencies.sortedBy { "${it.group}:${it.name}:${it.version}" }
    // Embedded NOTICE text -> the components carrying it.
    val notices = linkedMapOf<String, MutableList<String>>()

    val text = buildString {
      appendLine("Restate Kafka ingress integration")
      appendLine("Copyright (c) 2026 - Restate Software, Inc., Restate GmbH <code@restate.dev>")
      appendLine()
      appendLine(
          "This product is released under the MIT license (see the LICENSE file). It bundles"
      )
      appendLine("the third-party components listed below, each under the license(s) shown.")
      appendSection("Third-party components")
      for (module in modules) {
        val coordinates = "${module.group}:${module.name}:${module.version}"
        val licenses =
            module.poms
                .flatMap { it.licenses }
                .mapNotNull { l -> l.name?.let { if (l.url != null) "$it <${l.url}>" else it } }
                .ifEmpty { module.manifests.flatMap { it.licenses }.mapNotNull { it.name } }
                .distinct()
                .ifEmpty { listOf("Unknown") }
        appendLine(coordinates)
        licenses.forEach { appendLine("    $it") }
        module.licenseFiles
            .flatMap { it.fileDetails }
            .map { File(outputDir, it.file) }
            .filter { it.name.startsWith("NOTICE", ignoreCase = true) }
            .forEach { notices.getOrPut(it.readText().trim()) { mutableListOf() }.add(coordinates) }
      }
      appendSection("Third-party notices")
      appendLine("The NOTICE files of the bundled components, reproduced verbatim.")
      for ((notice, owners) in notices) {
        appendLine()
        appendLine("-".repeat(80))
        owners.forEach { appendLine(it) }
        appendLine("-".repeat(80))
        appendLine()
        appendLine(notice)
      }
    }
    File(outputDir, "NOTICE").writeText(text)
  }

  private fun StringBuilder.appendSection(title: String) {
    appendLine()
    appendLine("=".repeat(80))
    appendLine(title)
    appendLine("=".repeat(80))
    appendLine()
  }
}

// LICENSE and the generated NOTICE go to the image root (/LICENSE, /NOTICE), as in the restate
// server image.
val jibLegalDir = layout.buildDirectory.dir("jib-legal")
val prepareJibLegalFiles =
    tasks.register<Sync>("prepareJibLegalFiles") {
      dependsOn(tasks.generateLicenseReport)
      from("LICENSE")
      from("${licenseReport.absoluteOutputDir}/NOTICE")
      into(jibLegalDir)
    }

listOf("jib", "jibDockerBuild", "jibBuildTar").forEach {
  tasks.named(it) { dependsOn(prepareJibLegalFiles) }
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
    // Configure /app/extra-libs dir to be loaded in classpath.
    // This allows uses to load additional jars into the classpath, e.g. auth plugins, custom record
    // mappers.
    extraClasspath = listOf("/app/extra-libs/*")
    // Prometheus scrape endpoint (see RESTATE_METRICS_PORT); informational, does not publish it.
    ports = listOf("9464")
    // OCI annotations; these also override the base image's (e.g. its Ubuntu version).
    labels =
        mapOf(
            "org.opencontainers.image.title" to "Restate Kafka ingress integration",
            "org.opencontainers.image.description" to
                "Bridges the Kafka protocol into Restate, turning Kafka records into Restate invocations",
            "org.opencontainers.image.version" to version.toString(),
            "org.opencontainers.image.licenses" to "MIT",
            "org.opencontainers.image.source" to
                "https://github.com/restatedev/ingress-integration-kafka",
            "org.opencontainers.image.vendor" to "Restate Software, Inc., Restate GmbH",
        )
  }
  extraDirectories {
    paths {
      // Setting paths replaces the default, so keep src/main/jib (it provides /app/extra-libs).
      path { setFrom("src/main/jib") }
      path {
        setFrom(jibLegalDir)
        into = "/"
      }
    }
  }
}
