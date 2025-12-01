package ru.pospelov.etl.engine.exception;

import ru.pospelov.etl.engine.model.EtlRecord;

public class ExtractionException extends EtlException {

    public ExtractionException(String message, String jobId) {
        this(message, jobId, null, EtlErrorSeverity.CRITICAL, null);
    }

    public ExtractionException(String message, String jobId, Throwable cause) {
        this(message, jobId, null, EtlErrorSeverity.CRITICAL, cause);
    }

    public ExtractionException(
            String message,
            String jobId,
            EtlRecord record,
            EtlErrorSeverity severity
    ) {
        this(message, jobId, record, severity, null);
    }

    public ExtractionException(
            String message,
            String jobId,
            EtlRecord record,
            EtlErrorSeverity severity,
            Throwable cause
    ) {
        super(message, cause, jobId, EtlStage.EXTRACT, record, severity);
    }
}

