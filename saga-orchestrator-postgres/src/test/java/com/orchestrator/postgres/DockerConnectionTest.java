package com.orchestrator.postgres;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

class DockerConnectionTest {

    @Test
    void testDockerConnection() {
        try (GenericContainer<?> container = new GenericContainer<>("hello-world")) {
            container.start();
        }
    }
}
