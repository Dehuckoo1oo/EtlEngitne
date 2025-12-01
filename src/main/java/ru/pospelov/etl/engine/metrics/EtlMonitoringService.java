package ru.pospelov.etl.engine.metrics;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;

/**
 * Facade that exposes collected metrics and job statuses to other modules.
 */
@Service
public class EtlMonitoringService {

    private final EtlMetricsCollector collector;

    public EtlMonitoringService(EtlMetricsCollector collector) {
        this.collector = collector;
    }

    public Optional<EtlJobMetricsSnapshot> getMetrics(String jobId) {
        return collector.getSnapshot(jobId);
    }

    public Map<String, EtlJobMetricsSnapshot> getAllMetrics() {
        return collector.snapshotAll();
    }

    public Optional<EtlJobStatus> getStatus(String jobId) {
        return collector.getStatus(jobId);
    }

    public void registerListener(EtlMetrics listener) {
        collector.registerListener(listener);
    }

    public void unregisterListener(EtlMetrics listener) {
        collector.unregisterListener(listener);
    }

    public void clear(String jobId) {
        collector.clear(jobId);
    }

    public void clearAll() {
        collector.clearAll();
    }
}

