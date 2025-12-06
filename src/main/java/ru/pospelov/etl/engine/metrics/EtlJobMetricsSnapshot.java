package ru.pospelov.etl.engine.metrics;

import java.time.Duration;
import java.time.Instant;

/**
 * Immutable snapshot of metrics for a particular ETL job.
 */
public record EtlJobMetricsSnapshot(
        String jobId,
        EtlJobStatus status,
        Instant startedAt,
        Instant lastUpdatedAt,
        long extractedRecords,
        long processedRecords,
        long transformedRecords,
        long loadedRecords,
        long errorCount,
        long extractDurationMillis,
        long transformDurationMillis,
        long loadDurationMillis,
        long totalDurationMillis,  // Wall-clock time (real execution time)
        double throughput
) {

    /**
     * Returns the total wall-clock duration of the job execution.
     * For streaming pipelines, this is the actual time from start to finish,
     * not the sum of individual phase durations (which may overlap).
     */
    public Duration totalDuration() {
        return Duration.ofMillis(totalDurationMillis);
    }
}

