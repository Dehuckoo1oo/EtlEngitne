package ru.pospelov.etl.engine.metrics;

import ru.pospelov.etl.engine.model.EtlJob;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Хранилище метрик для одного задания. Оставлено package-private,
 * так как это деталь реализации {@link EtlMetricsCollector}.
 */
final class MutableJobMetrics {
    private final String jobId;
    private final AtomicReference<EtlJobStatus> status = new AtomicReference<>(EtlJobStatus.PENDING);
    private final AtomicLong startedAtMillis = new AtomicLong(0);
    private final AtomicLong lastUpdatedAtMillis = new AtomicLong(0);
    private final AtomicLong completedAtMillis = new AtomicLong(0);
    private final AtomicLong extractedRecords = new AtomicLong(0);
    private final AtomicLong processedRecords = new AtomicLong(0);
    private final AtomicLong transformedRecords = new AtomicLong(0);
    private final AtomicLong loadedRecords = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong extractDurationMillis = new AtomicLong(0);
    private final AtomicLong transformDurationMillis = new AtomicLong(0);
    private final AtomicLong loadDurationMillis = new AtomicLong(0);

    MutableJobMetrics(String jobId) {
        this.jobId = jobId;
    }

    void updateStatus(EtlJobStatus newStatus, Instant timestamp) {
        status.set(newStatus);
        long nowMillis = timestamp.toEpochMilli();
        if (newStatus != EtlJobStatus.PENDING) {
            startedAtMillis.compareAndSet(0, nowMillis);
        }
        if (newStatus == EtlJobStatus.COMPLETED || newStatus == EtlJobStatus.FAILED || newStatus == EtlJobStatus.CANCELLED) {
            completedAtMillis.set(nowMillis);
        }
        lastUpdatedAtMillis.set(nowMillis);
    }

    void updateExtract(long records, long durationMillis) {
        if (records > 0) {
            extractedRecords.set(records);
        }
        if (durationMillis > 0) {
            extractDurationMillis.set(durationMillis);
        }
        lastUpdatedAtMillis.set(System.currentTimeMillis());
    }

    void updateTransform(long records, long durationMillis) {
        if (records > 0) {
            transformedRecords.set(records);
        }
        if (durationMillis > 0) {
            transformDurationMillis.set(durationMillis);
        }
        lastUpdatedAtMillis.set(System.currentTimeMillis());
    }

    void updateLoad(long records, long durationMillis) {
        if (records > 0) {
            loadedRecords.set(records);
        }
        if (durationMillis > 0) {
            loadDurationMillis.set(durationMillis);
        }
        lastUpdatedAtMillis.set(System.currentTimeMillis());
    }

    void incrementProcessed() {
        processedRecords.incrementAndGet();
        lastUpdatedAtMillis.set(System.currentTimeMillis());
    }

    void incrementErrors() {
        errorCount.incrementAndGet();
        lastUpdatedAtMillis.set(System.currentTimeMillis());
    }

    EtlJobMetricsSnapshot toSnapshot() {
        long startedMillis = startedAtMillis.get();
        long updatedMillis = lastUpdatedAtMillis.get();
        long completedMillis = completedAtMillis.get();

        Instant started = startedMillis > 0 ? Instant.ofEpochMilli(startedMillis) : null;
        Instant updated = updatedMillis > 0 ? Instant.ofEpochMilli(updatedMillis) : null;
        Instant effectiveLastUpdated = updated != null ? updated : started;

        long totalDuration = computeTotalDuration(startedMillis, completedMillis, updatedMillis);
        double throughput = computeThroughput(totalDuration);

        return new EtlJobMetricsSnapshot(
                jobId,
                status.get(),
                started,
                effectiveLastUpdated,
                extractedRecords.get(),
                processedRecords.get(),
                transformedRecords.get(),
                loadedRecords.get(),
                errorCount.get(),
                extractDurationMillis.get(),
                transformDurationMillis.get(),
                loadDurationMillis.get(),
                totalDuration,
                throughput
        );
    }

    EtlJobStatus status() {
        return status.get();
    }

    private long computeTotalDuration(long startedMillis, long completedMillis, long updatedMillis) {
        if (startedMillis <= 0) {
            return 0;
        }
        if (completedMillis > 0) {
            return completedMillis - startedMillis;
        }
        if (updatedMillis > 0) {
            return updatedMillis - startedMillis;
        }
        return 0;
    }

    private double computeThroughput(long totalDurationMillis) {
        if (totalDurationMillis <= 0) {
            return 0d;
        }
        double seconds = totalDurationMillis / 1000d;
        long loaded = loadedRecords.get();
        if (loaded <= 0) {
            return 0d;
        }
        return loaded / seconds;
    }
}
