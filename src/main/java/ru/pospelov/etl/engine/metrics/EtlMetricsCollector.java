package ru.pospelov.etl.engine.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

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
        metrics.updateExtract(extractedRecords, durationMillis);
        publish(listener -> listener.onExtractComplete(job, extractedRecords, durationMillis));
    }

    @Override
    public void onTransformStart(EtlJob job, int inputRecords) {
        publish(listener -> listener.onTransformStart(job, inputRecords));
    }

    @Override
    public void onTransformComplete(EtlJob job, int outputRecords, long durationMillis) {
        MutableJobMetrics metrics = getOrCreate(job);
        metrics.updateTransform(outputRecords, durationMillis);
        publish(listener -> listener.onTransformComplete(job, outputRecords, durationMillis));
    }

    @Override
    public void onLoadStart(EtlJob job, int inputRecords) {
        publish(listener -> listener.onLoadStart(job, inputRecords));
    }

    @Override
    public void onLoadComplete(EtlJob job, int loadedRecords, long durationMillis) {
        MutableJobMetrics metrics = getOrCreate(job);
        metrics.updateLoad(loadedRecords, durationMillis);
        publish(listener -> listener.onLoadComplete(job, loadedRecords, durationMillis));
    }

    @Override
    public void onRecordProcessed(EtlJob job, EtlRecord record) {
        MutableJobMetrics metrics = getOrCreate(job);
        metrics.incrementProcessed();
        publish(listener -> listener.onRecordProcessed(job, record));
    }

    @Override
    public void onError(EtlJob job, EtlException exception) {
        MutableJobMetrics metrics = getOrCreate(job);
        metrics.incrementErrors();
        publish(listener -> listener.onError(job, exception));
    }

    private MutableJobMetrics getOrCreate(EtlJob job) {
        Objects.requireNonNull(job, "job must not be null");
        String jobId = job.jobId();
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
}
