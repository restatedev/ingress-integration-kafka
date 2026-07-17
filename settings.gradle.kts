plugins { id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0" }

rootProject.name = "ingress-integration-kafka"

// E2E-only: a Restate SDK service used as the invocation target in the integration test. It lives
// in
// its own module so the SDK's Vert.x 4.5 never touches the main app / test classpath (Vert.x 5).
include("e2e-greeter")
