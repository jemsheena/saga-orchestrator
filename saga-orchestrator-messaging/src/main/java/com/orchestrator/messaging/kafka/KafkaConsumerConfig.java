package com.orchestrator.messaging.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * Builds {@link Properties} for a Kafka {@code Consumer}.
 *
 * <p>See {@link KafkaProducerConfig}'s sandbox note — same caveat applies here.
 *
 * <p><b>{@code ENABLE_AUTO_COMMIT_CONFIG = false}:</b> commits are performed
 * manually by {@link KafkaMessageConsumer}, only after
 * {@code MessageHandler.handle} returns successfully — this is the actual
 * mechanism behind {@code MessageConsumer}'s documented at-least-once
 * contract. Auto-commit would ack a record's offset on a fixed timer
 * regardless of whether it was actually successfully processed, which is
 * incompatible with that contract.
 *
 * <p><b>{@code groupId} is a required parameter, not a fixed default:</b>
 * directly implements Milestone 3 architecture challenge resolution #1 —
 * independent consumer groups per participant reply topic, not one shared
 * group — by making every caller supply its own group id explicitly rather
 * than this class silently defaulting to something shared.
 *
 * <p><b>{@code CooperativeStickyAssignor}:</b> minimizes unnecessary
 * partition movement during a rebalance (Milestone 3 architecture review
 * Section 12/18) compared to the older eager range/round-robin assignors,
 * which revoke every partition from every consumer on any membership change.
 */
public final class KafkaConsumerConfig {

    private KafkaConsumerConfig() {
    }

    public static Properties build(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000); // generous - a slow handler must not trigger an unnecessary rebalance
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        return props;
    }
}
