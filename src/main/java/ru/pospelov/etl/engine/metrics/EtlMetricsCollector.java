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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
        // ОПТИМИЗАЦИЯ: Используем System.currentTimeMillis() вместо Instant.now()
        // для снижения overhead при частых вызовах
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
        private final AtomicReference<EtlJobStatus> status = new AtomicReference<>(EtlJobStatus.PENDING);
        // ОПТИМИЗАЦИЯ: Храним timestamp как long вместо Instant для снижения overhead
        private final AtomicLong startedAtMillis = new AtomicLong(0);
        private final AtomicLong lastUpdatedAtMillis = new AtomicLong(0);
        private final AtomicLong completedAtMillis = new AtomicLong(0);  // Время завершения для расчета wall-clock time
        private final AtomicLong extractedRecords = new AtomicLong(0);
        private final AtomicLong processedRecords = new AtomicLong(0);
        private final AtomicLong transformedRecords = new AtomicLong(0);
        private final AtomicLong loadedRecords = new AtomicLong(0);
        private final AtomicLong errorCount = new AtomicLong(0);
        private final AtomicLong extractDurationMillis = new AtomicLong(0);
        private final AtomicLong transformDurationMillis = new AtomicLong(0);
        private final AtomicLong loadDurationMillis = new AtomicLong(0);

        private MutableJobMetrics(String jobId) {
            this.jobId = jobId;
        }

        private void updateStatus(EtlJobStatus newStatus, Instant timestamp) {
            status.set(newStatus);
            long nowMillis = timestamp.toEpochMilli();
            if (newStatus != EtlJobStatus.PENDING) {
                startedAtMillis.compareAndSet(0, nowMillis);
            }
            // Отслеживаем время завершения для расчета wall-clock time
            if (newStatus == EtlJobStatus.COMPLETED || newStatus == EtlJobStatus.FAILED || newStatus == EtlJobStatus.CANCELLED) {
                completedAtMillis.set(nowMillis);
            }
            lastUpdatedAtMillis.set(nowMillis);
        }

        private void updateExtract(long records, long durationMillis) {
            // ОПТИМИЗАЦИЯ: Теперь принимаем абсолютные значения, не инкрементальные
            // Это позволяет вызывать метод реже (каждые N батчей)
            if (records > 0) {
                extractedRecords.set(records);  // Устанавливаем абсолютное значение
            }
            if (durationMillis > 0) {
                extractDurationMillis.set(durationMillis);
            }
            // Используем более легковесный способ обновления timestamp
            lastUpdatedAtMillis.set(System.currentTimeMillis());
        }

        private void updateTransform(long records, long durationMillis) {
            if (records > 0) {
                transformedRecords.set(records);  // Абсолютное значение
            }
            if (durationMillis > 0) {
                transformDurationMillis.set(durationMillis);  // Уже накопленное значение
            }
            lastUpdatedAtMillis.set(System.currentTimeMillis());
        }

        private void updateLoad(long records, long durationMillis) {
            if (records > 0) {
                loadedRecords.set(records);  // Абсолютное значение
            }
            if (durationMillis > 0) {
                loadDurationMillis.set(durationMillis);  // Уже накопленное значение
            }
            lastUpdatedAtMillis.set(System.currentTimeMillis());
        }

        private void incrementProcessed() {
            processedRecords.incrementAndGet();
            lastUpdatedAtMillis.set(System.currentTimeMillis());
        }

        private void incrementErrors() {
            errorCount.incrementAndGet();
            lastUpdatedAtMillis.set(System.currentTimeMillis());
        }

        private EtlJobMetricsSnapshot toSnapshot() {
            long startedMillis = startedAtMillis.get();
            long updatedMillis = lastUpdatedAtMillis.get();
            long completedMillis = completedAtMillis.get();

            Instant started = startedMillis > 0 ? Instant.ofEpochMilli(startedMillis) : null;
            Instant updated = updatedMillis > 0 ? Instant.ofEpochMilli(updatedMillis) : null;
            Instant effectiveLastUpdated = updated != null ? updated : started;

            // Вычисляем wall-clock time и throughput
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

        private EtlJobStatus status() {
            return status.get();
        }

        /**
         * Вычисляет реальное wall-clock время выполнения (от старта до завершения).
         * Для streaming pipeline это корректный способ расчета времени,
         * так как фазы extract/transform/load выполняются параллельно.
         */
        private long computeTotalDuration(long startedMillis, long completedMillis, long updatedMillis) {
            if (startedMillis <= 0) {
                return 0;
            }

            // Если джоб завершен (COMPLETED/FAILED/CANCELLED), используем время завершения
            if (completedMillis > 0) {
                return completedMillis - startedMillis;
            }

            // Если джоб ещё выполняется, используем текущее время
            if (updatedMillis > 0) {
                return updatedMillis - startedMillis;
            }

            return 0;
        }

        /**
         * Вычисляет throughput на основе wall-clock времени.
         */
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
}

