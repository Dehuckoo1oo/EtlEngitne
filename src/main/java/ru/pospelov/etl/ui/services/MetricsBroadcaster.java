package ru.pospelov.etl.ui.services;

import com.vaadin.flow.component.UI;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetrics;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Сервис для broadcast метрик ETL job'ов в UI через WebSocket Push.
 * Подписывается на события метрик и уведомляет подписчиков.
 */
@Slf4j
@Service
public class MetricsBroadcaster implements EtlMetrics {

    private final EtlMetricsCollector metricsCollector;
    private final Map<String, Consumer<String>> subscribers = new ConcurrentHashMap<>();

    public MetricsBroadcaster(EtlMetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    /**
     * Регистрируется как listener метрик после создания
     */
    @PostConstruct
    public void init() {
        metricsCollector.registerListener(this);
        log.info("MetricsBroadcaster initialized and registered as metrics listener");
    }

    /**
     * Подписывается на обновления метрик для конкретного job'а
     *
     * @param jobId ID job'а
     * @param ui UI для обновления
     * @param callback Callback для выполнения при обновлении
     */
    public void subscribe(String jobId, UI ui, Runnable callback) {
        String key = ui.getUIId() + ":" + jobId;
        subscribers.put(key, (jobIdEvent) -> {
            if (jobIdEvent.equals(jobId)) {
                ui.access(callback::run);
            }
        });
        log.debug("Subscribed UI {} to job {}", ui.getUIId(), jobId);
    }

    /**
     * Отписывается от обновлений
     */
    public void unsubscribe(String jobId, UI ui) {
        String key = ui.getUIId() + ":" + jobId;
        subscribers.remove(key);
        log.debug("Unsubscribed UI {} from job {}", ui.getUIId(), jobId);
    }

    /**
     * Уведомляет подписчиков об изменении
     */
    private void notifySubscribers(String jobId) {
        subscribers.values().forEach(consumer -> {
            try {
                consumer.accept(jobId);
            } catch (Exception e) {
                log.warn("Failed to notify subscriber for job {}: {}", jobId, e.getMessage());
            }
        });
    }

    // Реализация EtlMetrics интерфейса

    @Override
    public void onJobStatusChanged(EtlJob job, EtlJobStatus status) {
        log.debug("Job {} status changed to {}", job.getJobId(), status);
        notifySubscribers(job.getJobId());
    }

    @Override
    public void onExtractStart(EtlJob job) {
        notifySubscribers(job.getJobId());
    }

    @Override
    public void onExtractComplete(EtlJob job, int extractedRecords, long durationMillis) {
        notifySubscribers(job.getJobId());
    }

    @Override
    public void onTransformStart(EtlJob job, int inputRecords) {
        notifySubscribers(job.getJobId());
    }

    @Override
    public void onTransformComplete(EtlJob job, int outputRecords, long durationMillis) {
        notifySubscribers(job.getJobId());
    }

    @Override
    public void onLoadStart(EtlJob job, int inputRecords) {
        notifySubscribers(job.getJobId());
    }

    @Override
    public void onLoadComplete(EtlJob job, int loadedRecords, long durationMillis) {
        notifySubscribers(job.getJobId());
    }

    @Override
    public void onRecordProcessed(EtlJob job, EtlRecord record) {
        // Не уведомляем на каждую запись - слишком частые обновления
    }

    @Override
    public void onError(EtlJob job, EtlException exception) {
        notifySubscribers(job.getJobId());
    }
}
