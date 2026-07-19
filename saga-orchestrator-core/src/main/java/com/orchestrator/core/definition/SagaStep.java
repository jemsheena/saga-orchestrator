package com.orchestrator.core.definition;

import java.util.Objects;

/**
 * Represents a single step in a saga: the forward action and its compensation.
 *
 * <p>This is a pure value object. It carries no execution logic and no state —
 * it only describes WHAT should happen, never HOW or WHEN. The "how/when" is
 * the runtime engine's job (Milestone 3+), which is precisely why this class
 * has no methods beyond validation and accessors.
 *
 * <p>Design decision: {@code commandType} and {@code compensationType} are
 * Strings, not Class references or enums, on purpose. A saga definition must
 * be serializable and storable in the {@code saga_definition} table as JSON
 * (see schema), and must remain valid even if the Java class implementing a
 * given command handler is refactored/renamed. Coupling the definition to a
 * concrete Java type would break that. This is the same reason event-sourced
 * systems store event *type names*, not class references.
 *
 * @param stepName          unique, human-readable identifier for this step within
 *                          its saga definition (e.g. "ChargePayment"). Used in logs,
 *                          traces, and the saga_event audit trail — this is what a
 *                          support engineer will see at 2am, so it must be meaningful.
 * @param commandType       logical name of the command sent to the participant to
 *                          execute this step (e.g. "ChargePaymentCommand").
 * @param compensationType  logical name of the command sent to undo this step if a
 *                          LATER step fails (e.g. "RefundPaymentCommand"). Nullable
 *                          only for steps that are naturally non-compensatable —
 *                          see {@link #isCompensatable()}.
 */
public record SagaStep(
        String stepName,
        String commandType,
        String compensationType
) {

    /**
     * Compact constructor: enforces invariants at construction time so an
     * invalid SagaStep can never exist in memory. This is deliberate defensive
     * design — we want failures to happen at saga-definition-registration time,
     * not three services deep into a running saga at 2am.
     */
    public SagaStep {
        Objects.requireNonNull(stepName, "stepName must not be null");
        Objects.requireNonNull(commandType, "commandType must not be null");

        if (stepName.isBlank()) {
            throw new IllegalArgumentException("stepName must not be blank");
        }
        if (commandType.isBlank()) {
            throw new IllegalArgumentException("commandType must not be blank");
        }
        // compensationType is intentionally allowed to be null/blank — see isCompensatable().
    }

    /**
     * A step with no compensation is a legitimate, deliberate design choice —
     * not a bug. Example: a "SendConfirmationEmail" step is not worth undoing;
     * sending a follow-up "ignore that email" is often worse UX than doing nothing.
     *
     * <p>Marking this explicitly (rather than silently no-op-ing on a null
     * compensationType) forces whoever registers a SagaDefinition to make that
     * decision consciously, and forces the compensation engine (Milestone 5)
     * to make an explicit choice rather than accidentally sending a null command.
     */
    public boolean isCompensatable() {
        return compensationType != null && !compensationType.isBlank();
    }
}
