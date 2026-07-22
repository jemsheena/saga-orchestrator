package com.orchestrator.messaging.proto;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtobufContractTest {

    @Test
    void sagaCommand_roundTripsThroughSerializedBytesExactly() throws InvalidProtocolBufferException {
        SagaCommand original = SagaCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setSagaId(UUID.randomUUID().toString())
                .setStepName("ChargePayment")
                .setCommandType("ChargePaymentCommand")
                .setPayload(ByteString.copyFromUtf8("{\"amount\":100}"))
                .build();

        SagaCommand rebuilt = SagaCommand.parseFrom(original.toByteArray());

        assertEquals(original, rebuilt);
        assertEquals("ChargePayment", rebuilt.getStepName());
        assertEquals("{\"amount\":100}", rebuilt.getPayload().toStringUtf8());
    }

    @Test
    void sagaReply_success_roundTrips_reasonStaysEmpty() throws InvalidProtocolBufferException {
        SagaReply original = SagaReply.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSagaId(UUID.randomUUID().toString())
                .setStepName("ChargePayment")
                .setOutcome(SagaReply.Outcome.SUCCESS)
                .build();

        SagaReply rebuilt = SagaReply.parseFrom(original.toByteArray());

        assertEquals(SagaReply.Outcome.SUCCESS, rebuilt.getOutcome());
        assertTrue(rebuilt.getReason().isEmpty());
    }

    @Test
    void sagaReply_failure_roundTripsWithReasonIntact() throws InvalidProtocolBufferException {
        SagaReply original = SagaReply.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSagaId(UUID.randomUUID().toString())
                .setStepName("CreateShippingLabel")
                .setOutcome(SagaReply.Outcome.FAILURE)
                .setReason("carrier API down")
                .build();

        SagaReply rebuilt = SagaReply.parseFrom(original.toByteArray());

        assertEquals(SagaReply.Outcome.FAILURE, rebuilt.getOutcome());
        assertEquals("carrier API down", rebuilt.getReason());
    }

    @Test
    void unsetOutcome_defaultsToUnspecified() {
        SagaReply noOutcomeSet = SagaReply.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setSagaId(UUID.randomUUID().toString())
                .setStepName("X")
                .build();

        assertEquals(SagaReply.Outcome.OUTCOME_UNSPECIFIED, noOutcomeSet.getOutcome());
    }

    @Test
    void malformedBytes_throwInvalidProtocolBufferException_notSilentWrongParse() {
        byte[] garbage = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x02, 0x01};
        assertThrows(InvalidProtocolBufferException.class, () -> SagaCommand.parseFrom(garbage));
    }

    @Test
    void unsetOptionalFields_parseAsEmptyStrings_notNull_forwardCompatibilityBaseline() throws InvalidProtocolBufferException {
        SagaCommand minimal = SagaCommand.newBuilder()
                .setCommandId(UUID.randomUUID().toString())
                .setSagaId(UUID.randomUUID().toString())
                .build();

        SagaCommand rebuilt = SagaCommand.parseFrom(minimal.toByteArray());

        assertTrue(rebuilt.getStepName().isEmpty());
        assertTrue(rebuilt.getCommandType().isEmpty());
    }
}
