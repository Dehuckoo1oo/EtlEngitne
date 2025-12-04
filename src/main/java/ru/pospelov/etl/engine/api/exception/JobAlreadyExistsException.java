package ru.pospelov.etl.engine.api.exception;

/**
 * Исключение, выбрасываемое когда пытаются создать job с уже существующим id.
 */
public class JobAlreadyExistsException extends RuntimeException {

    public JobAlreadyExistsException(String jobId) {
        super("Job already exists with id: " + jobId);
    }

    public JobAlreadyExistsException(String jobId, Throwable cause) {
        super("Job already exists with id: " + jobId, cause);
    }
}
