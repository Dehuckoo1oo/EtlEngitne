package ru.pospelov.etl.engine.api.exception;

import ru.pospelov.etl.engine.metrics.EtlJobStatus;

/**
 * Исключение, выбрасываемое когда пытаются запустить job, который уже выполняется.
 */
public class JobAlreadyRunningException extends RuntimeException {

    private final String jobId;
    private final EtlJobStatus currentStatus;

    public JobAlreadyRunningException(String jobId, EtlJobStatus currentStatus) {
        super(String.format("Job '%s' is already running with status: %s", jobId, currentStatus));
        this.jobId = jobId;
        this.currentStatus = currentStatus;
    }

    public String getJobId() {
        return jobId;
    }

    public EtlJobStatus getCurrentStatus() {
        return currentStatus;
    }
}
