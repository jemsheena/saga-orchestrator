package com.orchestrator.messaging.outbox;

import com.orchestrator.messaging.MessageHeaders;
import com.orchestrator.messaging.MessagePublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/** Test-only {@link MessagePublisher} that records every call and can be configured to fail for specific keys. */
public final class InMemoryMessagePublisher implements MessagePublisher {

    public record PublishedMessage(String topic, String key, byte[] payload, MessageHeaders headers) {
    }

    private final List<PublishedMessage> published = new ArrayList<>();
    private Predicate<String> failForKey = key -> false;

    @Override
    public synchronized void publish(String topic, String key, byte[] payload, MessageHeaders headers) {
        if (failForKey.test(key)) {
            throw new RuntimeException("simulated publish failure for key " + key);
        }
        published.add(new PublishedMessage(topic, key, payload, headers));
    }

    public synchronized List<PublishedMessage> published() {
        return List.copyOf(published);
    }

    public void failForKeys(Set<String> keys) {
        this.failForKey = keys::contains;
    }
}
