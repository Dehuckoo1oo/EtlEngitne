package ru.pospelov.etl.engine.engine;

/**
 * Exception thrown when ETL pipeline execution is cancelled.
 */
public class CancellationException extends RuntimeException {
    
    public CancellationException(String message) {
        super(message);
    }
    
    public CancellationException(String message, Throwable cause) {
        super(message, cause);
    }
}

