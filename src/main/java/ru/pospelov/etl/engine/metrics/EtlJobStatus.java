package ru.pospelov.etl.engine.metrics;

/**
 * High-level execution states that a job can be in during its lifecycle.
 */
public enum EtlJobStatus {
    /**
     * The job is registered but execution has not started yet.
     */
    PENDING,

    /**
     * The pipeline is actively running but the stage is not specified.
     */
    RUNNING,

    /**
     * Extraction stage is in progress.
     */
    EXTRACTING,

    /**
     * Transformation stage is in progress.
     */
    TRANSFORMING,

    /**
     * Loading stage is in progress.
     */
    LOADING,

    /**
     * Execution finished successfully.
     */
    COMPLETED,

    /**
     * Execution terminated due to an unrecoverable error.
     */
    FAILED,

    /**
     * Execution was cancelled by the user/system.
     */
    CANCELLED,

    /**
     * Execution stopped because of a timeout.
     */
    TIMEOUT
}

