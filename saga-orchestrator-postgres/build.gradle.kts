plugins {
    java
}

group = "com.orchestrator"
version = "0.1.0-M2"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":saga-orchestrator-core"))
    implementation(project(":saga-orchestrator-messaging"))

    // Actual PostgreSQL JDBC driver - needed at RUNTIME to connect, not to
    // compile against this module's use of the java.sql/javax.sql interfaces
    // (those are JDK-bundled). Version pinned; bump alongside PostgreSQL
    // server version compatibility checks.
    runtimeOnly("org.postgresql:postgresql:42.7.4")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // Testcontainers: spins up a real, ephemeral PostgreSQL for integration
    // tests (PostgresSagaEventStoreIntegrationTest, etc.) rather than mocking
    // JDBC by hand. Requires local Docker to actually run.
    testImplementation("org.testcontainers:junit-jupiter:1.20.1")
    testImplementation("org.testcontainers:postgresql:1.20.1")
    testImplementation("org.postgresql:postgresql:42.7.4")
}

tasks.test {
    useJUnitPlatform()
}
