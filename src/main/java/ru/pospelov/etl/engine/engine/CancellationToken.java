package ru.pospelov.etl.engine.engine;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-safe token for cancelling ETL pipeline execution.
 * Can be checked at any point during execution to determine if cancellation was requested.
 */
public class CancellationToken {
    
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    
    /**
     * Requests cancellation of the execution.
     * This operation is idempotent - multiple calls have the same effect.
     */
    public void cancel() {
        cancelled.set(true);
    }
    
    /**
     * Checks if cancellation has been requested.
     * 
     * @return true if cancellation was requested, false otherwise
     */
    public boolean isCancelled() {
        return cancelled.get();
    }
    
    /**
     * Resets the cancellation state. Should be used with caution.
     * Typically used when reusing a token for a new execution.
     */
    public void reset() {
        cancelled.set(false);
    }
    
    /**
     * Throws CancellationException if cancellation was requested.
     * 
     * @throws CancellationException if cancellation was requested
     */
    public void checkCancellation() {
        if (isCancelled()) {
            throw new CancellationException("Execution was cancelled");
        }
    }
}

