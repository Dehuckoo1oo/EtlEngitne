package ru.pospelov.etl.engine.engine;

import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.EtlStage;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.time.Instant;

public record DeadLetterEntry(
        String jobId,
        EtlStage stage,
        EtlRecord record,
        EtlErrorSeverity severity,
        String message,
        Instant timestamp,
        Throwable cause
) {
    public static DeadLetterEntry fromException(EtlException exception) {
        return new DeadLetterEntry(
                exception.getJobId(),
                exception.getStage(),
                exception.getRecord(),
                exception.getSeverity(),
                exception.getMessage(),
                Instant.now(),
                exception.getCause()
        );
    }
}

