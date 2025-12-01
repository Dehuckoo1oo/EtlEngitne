package ru.pospelov.etl.engine.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Default in-memory implementation that aggregates ETL metrics
 * and allows external listeners to subscribe to metric events.
 */
@Component
public class EtlMetricsCollector implements EtlMetrics {

    private static final Logger log = LoggerFactory.getLogger(EtlMetricsCollector.class);

    private final ConcurrentMap<String, MutableJobMetrics> metricsByJob = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<EtlMetrics> listeners = new CopyOnWriteArrayList<>();

    public void registerListener(EtlMetrics listener) {
        Objects.requireNonNull(listener, "listener");
        listeners.addIfAbsent(listener);
    }

    public void unregisterListener(EtlMetrics listener) {
        if (listener == null) {
            return;
        }
        listeners.remove(listener);
    }

    public Optional<EtlJobMetricsSnapshot> getSnapshot(String jobId) {
        MutableJobMetrics metrics = metricsByJob.get(jobId);
        return metrics == null ? Optional.empty() : Optional.of(metrics.toSnapshot());
    }

    public Map<String, EtlJobMetricsSnapshot> snapshotAll() {
        Map<String, EtlJobMetricsSnapshot> snapshotByJob = new HashMap<>();
        metricsByJob.forEach((jobId, metrics) -> snapshotByJob.put(jobId, metrics.toSnapshot()));
        return Map.copyOf(snapshotByJob);
    }

    public Optional<EtlJobStatus> getStatus(String jobId) {
        return Optional.ofNullable(metricsByJob.get(jobId))
                .map(MutableJobMetrics::status);
    }

    public void clear(String jobId) {
        metricsByJob.remove(jobId);
    }

    public void clearAll() {
        metricsByJob.clear();
    }

    @Override
    public void onJobStatusChanged(EtlJob job, EtlJobStatus status) {
        MutableJobMetrics metrics = getOrCreate(job);
        Instant now = Instant.now();
        metrics.updateStatus(status, now);
        publish(listener -> listener.onJobStatusChanged(job, status));
    }

    @Override
    public void onExtractStart(EtlJob job) {
        publish(listener -> listener.onExtractStart(job));
    }

    @Override
    public void onExtractComplete(EtlJob job, int extractedRecords, long durationMillis) {
        MutableJobMetrics metrics = getOrCreate(job);
        Instant now = Instant.now();
        metrics.updateExtract(extractedRecords, durationMillis, now);
        publish(listener -> listener.onExtractComplete(job, extractedRecords, durationMillis));
    }

    @Override
    public void onTransformStart(EtlJob job, int inputRecords) {
        publish(listener -> listener.onTransformStart(job, inputRecords));
    }

    @Override
    public void onTransformComplete(EtlJob job, int outputRecords, long durationMillis) {
        MutableJobMetrics metrics = getOrCreate(job);
        Instant now = Instant.now();
        metrics.updateTransform(outputRecords, durationMillis, now);
        publish(listener -> listener.onTransformComplete(job, outputRecords, durationMillis));
    }

    @Override
    public void onLoadStart(EtlJob job, int inputRecords) {
        publish(listener -> listener.onLoadStart(job, inputRecords));
    }

    @Override
    public void onLoadComplete(EtlJob job, int loadedRecords, long durationMillis) {
        MutableJobMetrics metrics = getOrCreate(job);
        Instant now = Instant.now();
        metrics.updateLoad(loadedRecords, durationMillis, now);
        publish(listener -> listener.onLoadComplete(job, loadedRecords, durationMillis));
    }

    @Override
    public void onRecordProcessed(EtlJob job, EtlRecord record) {
        MutableJobMetrics metrics = getOrCreate(job);
        Instant now = Instant.now();
        metrics.incrementProcessed(now);
        publish(listener -> listener.onRecordProcessed(job, record));
    }

    @Override
    public void onError(EtlJob job, EtlException exception) {
        MutableJobMetrics metrics = getOrCreate(job);
        Instant now = Instant.now();
        metrics.incrementErrors(now);
        publish(listener -> listener.onError(job, exception));
    }

    private MutableJobMetrics getOrCreate(EtlJob job) {
        Objects.requireNonNull(job, "job must not be null");
        String jobId = job.getJobId();
        if (jobId == null || jobId.isBlank()) {
            jobId = "unknown";
        }
        final String finalJobId = jobId;
        return metricsByJob.computeIfAbsent(finalJobId, MutableJobMetrics::new);
    }

    private void publish(Consumer<EtlMetrics> callback) {
        if (listeners.isEmpty()) {
            return;
        }
        for (EtlMetrics listener : listeners) {
            try {
                callback.accept(listener);
            } catch (Exception e) {
                log.warn("Metrics listener {} failed: {}", listener, e.getMessage(), e);
            }
        }
    }

    private static final class MutableJobMetrics {
        private final String jobId;
        private EtlJobStatus status = EtlJobStatus.PENDING;
        private Instant startedAt;
        private Instant lastUpdatedAt;
        private long extractedRecords;
        private long processedRecords;
        private long transformedRecords;
        private long loadedRecords;
        private long errorCount;
        private long extractDurationMillis;
        private long transformDurationMillis;
        private long loadDurationMillis;

        private MutableJobMetrics(String jobId) {
            this.jobId = jobId;
        }

        private synchronized void updateStatus(EtlJobStatus newStatus, Instant timestamp) {
            status = newStatus;
            if (startedAt == null && newStatus != EtlJobStatus.PENDING) {
                startedAt = timestamp;
            }
            lastUpdatedAt = timestamp;
        }

        private synchronized void updateExtract(long records, long durationMillis, Instant timestamp) {
            extractedRecords = Math.max(records, 0);
            extractDurationMillis = Math.max(durationMillis, 0);
            lastUpdatedAt = timestamp;
        }

        private synchronized void updateTransform(long records, long durationMillis, Instant timestamp) {
            transformedRecords = Math.max(records, 0);
            transformDurationMillis = Math.max(durationMillis, 0);
            lastUpdatedAt = timestamp;
        }

        private synchronized void updateLoad(long records, long durationMillis, Instant timestamp) {
            loadedRecords = Math.max(records, 0);
            loadDurationMillis = Math.max(durationMillis, 0);
            lastUpdatedAt = timestamp;
        }

        private synchronized void incrementProcessed(Instant timestamp) {
            processedRecords++;
            lastUpdatedAt = timestamp;
        }

        private synchronized void incrementErrors(Instant timestamp) {
            errorCount++;
            lastUpdatedAt = timestamp;
        }

        private synchronized EtlJobMetricsSnapshot toSnapshot() {
            Instant effectiveLastUpdated = lastUpdatedAt != null ? lastUpdatedAt : startedAt;
            double throughput = computeThroughput(effectiveLastUpdated);
            return new EtlJobMetricsSnapshot(
                    jobId,
                    status,
                    startedAt,
                    effectiveLastUpdated,
                    extractedRecords,
                    processedRecords,
                    transformedRecords,
                    loadedRecords,
                    errorCount,
                    extractDurationMillis,
                    transformDurationMillis,
                    loadDurationMillis,
                    throughput
            );
        }

        private synchronized EtlJobStatus status() {
            return status;
        }

        private double computeThroughput(Instant updatedAt) {
            if (startedAt == null || updatedAt == null) {
                return 0d;
            }
            Duration totalDuration = Duration.between(startedAt, updatedAt);
            if (totalDuration.isZero() || totalDuration.isNegative()) {
                return 0d;
            }
            double seconds = totalDuration.toMillis() / 1000d;
            if (seconds <= 0d) {
                return 0d;
            }
            return loadedRecords / seconds;
        }
    }
}

