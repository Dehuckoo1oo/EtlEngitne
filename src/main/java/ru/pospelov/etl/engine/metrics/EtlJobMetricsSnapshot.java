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
        double throughput
) {

    public Duration totalDuration() {
        if (startedAt == null || lastUpdatedAt == null) {
            return Duration.ZERO;
        }
        return Duration.between(startedAt, lastUpdatedAt);
    }
}

