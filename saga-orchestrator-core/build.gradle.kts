plugins {
    java
}

group = "com.orchestrator"
version = "0.1.0-M1"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // Milestone 1 has ZERO runtime dependencies on purpose — the domain model
    // must not depend on Spring, Kafka, or Jackson. Coupling it to a framework
    // this early would mean we're testing the framework, not our own logic.
    // Frameworks get wired in at the boundary (Milestone 3+ modules), never here.

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
