package ru.pospelov.etl.engine.exception;

import ru.pospelov.etl.engine.model.EtlRecord;

public class TransformationException extends EtlException {

    public TransformationException(String message, String jobId, EtlRecord record) {
        this(message, jobId, record, EtlErrorSeverity.CRITICAL, null);
    }

    public TransformationException(
            String message,
            String jobId,
            EtlRecord record,
            EtlErrorSeverity severity
    ) {
        this(message, jobId, record, severity, null);
    }

    public TransformationException(
            String message,
            String jobId,
            EtlRecord record,
            EtlErrorSeverity severity,
            Throwable cause
    ) {
        super(message, cause, jobId, EtlStage.TRANSFORM, record, severity);
    }
}

