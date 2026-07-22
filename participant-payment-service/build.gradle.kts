plugins {
    java
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
    implementation(project(":saga-orchestrator-messaging"))
    implementation("com.google.protobuf:protobuf-java:3.25.3")
    implementation("org.apache.kafka:kafka-clients:3.7.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
