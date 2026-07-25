package com.orchestrator.postgres;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.DockerClientFactory;

class DockerConnectionTest {

    @Test
    void testDockerConnection() {
        Assumptions.assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker not available - skipping test");
        try (GenericContainer<?> container = new GenericContainer<>("hello-world")) {
            container.start();
        }
    }
}
