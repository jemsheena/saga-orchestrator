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

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":saga-orchestrator-core"))
    implementation(project(":saga-orchestrator-messaging"))
    implementation("com.google.protobuf:protobuf-java:3.25.3")
    implementation("io.micrometer:micrometer-core:1.12.0")

    // Actual PostgreSQL JDBC driver - needed at RUNTIME to connect, not to
    // compile against this module's use of the java.sql/javax.sql interfaces
    // (those are JDK-bundled). Version pinned; bump alongside PostgreSQL
    // server version compatibility checks.
    runtimeOnly("org.postgresql:postgresql:42.7.4")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.9")

    // Testcontainers: spins up a real, ephemeral PostgreSQL for integration
    // tests (PostgresSagaEventStoreIntegrationTest, etc.) rather than mocking
    // JDBC by hand. Requires local Docker to actually run.
    // Pin Testcontainers to the current stable release to ensure consistent
    // versions across modules while BOM resolution is deferred.
    val testcontainersVersion = "2.0.5"
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:$testcontainersVersion")
    testImplementation("org.testcontainers:testcontainers-postgresql:$testcontainersVersion")
    testImplementation("org.postgresql:postgresql:42.7.4")
}

tasks.test {
    useJUnitPlatform()
}
