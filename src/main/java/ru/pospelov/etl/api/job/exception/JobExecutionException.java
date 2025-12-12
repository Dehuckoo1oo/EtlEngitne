package ru.pospelov.etl.api.job.exception;

/**
 * Исключение, выбрасываемое при ошибках выполнения job'а.
 */
public class JobExecutionException extends RuntimeException {

    public JobExecutionException(String message) {
        super(message);
    }

    public JobExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
