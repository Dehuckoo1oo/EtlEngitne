package ru.pospelov.etl.engine.api.exception;

/**
 * Исключение, выбрасываемое когда запрошенный job не найден в репозитории.
 */
public class JobNotFoundException extends RuntimeException {

    public JobNotFoundException(String jobId) {
        super("Job not found with id: " + jobId);
    }

    public JobNotFoundException(String jobId, Throwable cause) {
        super("Job not found with id: " + jobId, cause);
    }
}
