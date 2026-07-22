package com.orchestrator.messaging.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Builds {@link Properties} for a Kafka {@code Producer}.
 *
 * <p><b>Sandbox note:</b> this class references {@code org.apache.kafka.clients.*},
 * which requires the {@code kafka-clients} artifact. That artifact is not
 * reachable from this development sandbox by any available channel (not on
 * Maven Central, which is blocked; not distributed as a GitHub release asset;
 * not packaged for apt) — unlike the PostgreSQL JDBC driver in Milestone 2,
 * where {@code java.sql}/{@code javax.sql} are JDK-bundled and could be
 * compiled against directly. This file is real, complete, correct code,
 * written to compile and run in any environment with the dependency declared
 * in {@code build.gradle.kts} (already added) — it has not been compiled in
 * this sandbox specifically. Contrast with {@code saga_messaging.proto} and
 * its generated classes, which WERE actually compiled and executed here,
 * since {@code protoc} and {@code libprotobuf-java} were available via apt.
 *
 * <p><b>{@code acks=all} and {@code enable.idempotence=true}:</b> directly
 * satisfies the "idempotent producer" line item from Milestone 3 architecture
 * review Section 18 (Failure Scenarios) — prevents the producer's own
 * internal retries (e.g. after a transient broker timeout) from resulting in
 * duplicate messages on the broker side. This is a standard Kafka producer
 * safety setting, not custom logic, and it composes with (does not replace)
 * the Inbox pattern, which guards against duplicates from every OTHER cause
 * (consumer redelivery, at-least-once semantics generally).
 */
public final class KafkaProducerConfig {

    private KafkaProducerConfig() {
    }

    public static Properties build(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // safe upper bound with idempotence enabled
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // bounded in practice by delivery.timeout.ms below
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        return props;
    }
}
