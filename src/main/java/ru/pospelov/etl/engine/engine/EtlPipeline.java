package ru.pospelov.etl.engine.engine;

import ru.pospelov.etl.engine.exception.CancellationException;
import ru.pospelov.etl.engine.model.EtlJob;

/**
 * ETL pipeline that processes data through extract, transform, and load stages.
 * Supports cancellation of execution.
 */
public interface EtlPipeline {
    /**
     * Executes the ETL pipeline for the given job.
     * 
     * @param job the ETL job to execute
     * @throws CancellationException if execution was cancelled
     */
    void run(EtlJob job);
    
    /**
     * Cancels the execution of the pipeline.
     * If the pipeline is not currently running, the next execution will be cancelled immediately.
     * If the pipeline is running, it will be cancelled at the next cancellation check point.
     */
    void cancel();
    
    /**
     * Checks if the pipeline execution has been cancelled.
     * 
     * @return true if cancellation was requested, false otherwise
     */
    boolean isCancelled();
}
