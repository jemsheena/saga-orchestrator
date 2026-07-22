plugins {
    java
    id("com.google.protobuf") version "0.9.4"
}

group = "com.orchestrator"
version = "0.1.0-M3"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // Pure generated-message dependency - protoc-generated Java classes need
    // this runtime regardless of what messaging technology carries the bytes.
    implementation("com.google.protobuf:protobuf-java:3.25.3")

    // Kafka producer/consumer client. Used only by the com.orchestrator.messaging.kafka
    // package - the rest of this module (MessagePublisher/MessageHandler interfaces,
    // Outbox, Inbox) has no dependency on this at all, by design - see module javadoc.
    implementation("org.apache.kafka:kafka-clients:3.7.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.9")

    // Testcontainers Kafka module - spins up a real, ephemeral Kafka broker for
    // integration tests. Requires local Docker to actually run.
    // Pin Testcontainers to the current stable release to ensure consistent
    // versions across modules while BOM resolution is deferred.
    val testcontainersVersion = "2.0.5"
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:$testcontainersVersion")
    testImplementation("org.testcontainers:testcontainers-kafka:$testcontainersVersion")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.3"
    }
}

tasks.test {
    useJUnitPlatform()
}
