package ru.pospelov.etl.engine.exception;

import ru.pospelov.etl.engine.model.EtlRecord;

public class LoadingException extends EtlException {

    public LoadingException(String message, String jobId, EtlRecord record) {
        this(message, jobId, record, EtlErrorSeverity.CRITICAL, null);
    }

    public LoadingException(
            String message,
            String jobId,
            EtlRecord record,
            EtlErrorSeverity severity
    ) {
        this(message, jobId, record, severity, null);
    }

    public LoadingException(
            String message,
            String jobId,
            EtlRecord record,
            EtlErrorSeverity severity,
            Throwable cause
    ) {
        super(message, cause, jobId, EtlStage.LOAD, record, severity);
    }
}

