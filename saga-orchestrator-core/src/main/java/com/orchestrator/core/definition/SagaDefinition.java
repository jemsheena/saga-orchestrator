package com.orchestrator.core.definition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The immutable blueprint for a saga type: an ordered list of {@link SagaStep}s
 * plus a version number.
 *
 * <p><b>Why versioning is a first-class field, not an afterthought:</b>
 * A saga in production might run for minutes (order fulfillment) or days
 * (a multi-week onboarding workflow). If you deploy a new version of the
 * definition — reordering steps, adding a step — while old instances are
 * still in flight, those in-flight instances must keep executing against
 * the definition version they started with, or their {@code currentStepIndex}
 * becomes meaningless. This is a real production bug class: "I changed the
 * workflow and every in-flight saga broke." We are designing it out from
 * day one rather than discovering it in Milestone 6.
 *
 * <p><b>Why Builder, not a public constructor or a static factory taking a List:</b>
 * A telescoping constructor (name, version, steps, timeoutPolicy-later, ...)
 * gets unreadable fast, and a raw {@code List<SagaStep>} constructor makes it
 * easy to accidentally pass an empty or unordered list. The Builder gives us
 * one place to enforce "a saga must have at least one step" and to make the
 * call site self-documenting: {@code SagaDefinition.builder("OrderFulfillment")
 * .addStep(...).addStep(...).build()} reads like the workflow itself.
 */
public final class SagaDefinition {

    private final String sagaType;
    private final int version;
    private final List<SagaStep> steps;

    private SagaDefinition(Builder builder) {
        this.sagaType = builder.sagaType;
        this.version = builder.version;
        // Defensive copy + immutable wrapper: once built, NOTHING can mutate
        // the step list, even if a caller kept a reference to the builder's
        // internal list. This matters because SagaDefinition instances are
        // meant to be cached and shared across every SagaInstance of that type —
        // a single accidental mutation would corrupt every in-flight saga.
        this.steps = Collections.unmodifiableList(new ArrayList<>(builder.steps));
    }

    public String sagaType() {
        return sagaType;
    }

    public int version() {
        return version;
    }

    public List<SagaStep> steps() {
        return steps;
    }

    public int stepCount() {
        return steps.size();
    }

    /**
     * @param index zero-based step index
     * @return the step at that index
     * @throws IndexOutOfBoundsException if index is out of range — deliberately
     *         NOT swallowed/defaulted, because an out-of-range step index means
     *         the SagaInstance's state is already corrupted upstream, and hiding
     *         that here would turn a loud, immediate bug into a silent, delayed one.
     */
    public SagaStep stepAt(int index) {
        return steps.get(index);
    }

    public boolean isLastStep(int index) {
        return index == steps.size() - 1;
    }

    /**
     * Derives this definition's identity reference — the {@code (sagaType, version)}
     * pair that a {@code SagaInstance} pins itself to at construction time and
     * validates every subsequent operation against. See {@link SagaDefinitionReference}
     * for the full reasoning on why instances store this instead of a live
     * {@code SagaDefinition} reference.
     */
    public SagaDefinitionReference reference() {
        return new SagaDefinitionReference(sagaType, version);
    }

    public static Builder builder(String sagaType) {
        return new Builder(sagaType);
    }

    public static final class Builder {
        private final String sagaType;
        private int version = 1;
        private final List<SagaStep> steps = new ArrayList<>();

        private Builder(String sagaType) {
            Objects.requireNonNull(sagaType, "sagaType must not be null");
            if (sagaType.isBlank()) {
                throw new IllegalArgumentException("sagaType must not be blank");
            }
            this.sagaType = sagaType;
        }

        public Builder version(int version) {
            if (version < 1) {
                throw new IllegalArgumentException("version must be >= 1");
            }
            this.version = version;
            return this;
        }

        public Builder addStep(SagaStep step) {
            Objects.requireNonNull(step, "step must not be null");
            for (SagaStep existing : steps) {
                if (existing.stepName().equals(step.stepName())) {
                    throw new IllegalArgumentException(
                            "Duplicate stepName within a single saga definition: " + step.stepName()
                                    + " — step names must be unique because they are used as the "
                                    + "correlation key in the saga_event audit trail.");
                }
            }
            steps.add(step);
            return this;
        }

        /**
         * Builds and validates the definition. A saga with zero steps is not
         * a valid workflow — it would immediately "complete" without doing
         * anything, which is almost certainly a configuration mistake, not
         * an intentional design, so we fail loudly here rather than let it
         * silently no-op in production.
         */
        public SagaDefinition build() {
            if (steps.isEmpty()) {
                throw new IllegalStateException(
                        "SagaDefinition '" + sagaType + "' must contain at least one step");
            }
            return new SagaDefinition(this);
        }
    }
}
