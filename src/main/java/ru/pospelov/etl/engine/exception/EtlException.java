package ru.pospelov.etl.engine.exception;

import org.springframework.util.StringUtils;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Objects;
import java.util.Optional;

/**
 * Base class for all structured ETL exceptions that carries execution context.
 */
public class EtlException extends RuntimeException {

    private final EtlStage stage;
    private final EtlErrorSeverity severity;
    private final String jobId;
    private final EtlRecord record;

    public EtlException(
            String message,
            String jobId,
            EtlStage stage,
            EtlRecord record,
            EtlErrorSeverity severity
    ) {
        super(message);
        this.stage = Objects.requireNonNull(stage, "stage must not be null");
        this.severity = severity == null ? EtlErrorSeverity.CRITICAL : severity;
        this.jobId = StringUtils.hasText(jobId) ? jobId : "unknown";
        this.record = record;
    }

    public EtlException(
            String message,
            Throwable cause,
            String jobId,
            EtlStage stage,
            EtlRecord record,
            EtlErrorSeverity severity
    ) {
        super(message, cause);
        this.stage = Objects.requireNonNull(stage, "stage must not be null");
        this.severity = severity == null ? EtlErrorSeverity.CRITICAL : severity;
        this.jobId = StringUtils.hasText(jobId) ? jobId : "unknown";
        this.record = record;
    }

    public EtlStage getStage() {
        return stage;
    }

    public EtlErrorSeverity getSeverity() {
        return severity;
    }

    public String getJobId() {
        return jobId;
    }

    public EtlRecord getRecord() {
        return record;
    }

    public Optional<EtlRecord> getRecordSafe() {
        return Optional.ofNullable(record);
    }

    public boolean isCritical() {
        return severity == EtlErrorSeverity.CRITICAL;
    }
}

