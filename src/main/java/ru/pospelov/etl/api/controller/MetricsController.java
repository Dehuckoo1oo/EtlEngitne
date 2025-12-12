package ru.pospelov.etl.api.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.pospelov.etl.engine.metrics.EtlJobMetricsSnapshot;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;

import java.util.Map;
import java.util.Optional;

/**
 * REST контроллер для получения метрик и статусов ETL job'ов.
 *
 * Предоставляет следующие эндпоинты:
 * - GET /api/metrics/{jobId}        - получить метрики по конкретному job'у
 * - GET /api/metrics                - получить метрики по всем job'ам
 * - GET /api/metrics/{jobId}/status - получить статус конкретного job'а
 */
@Slf4j
@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    private final EtlMetricsCollector metricsCollector;

    public MetricsController(EtlMetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    /**
     * Получить метрики для конкретного job'а
     *
     * @param jobId идентификатор job'а
     * @return snapshot с метриками или 404 если job не найден
     */
    @GetMapping("/{jobId}")
    public ResponseEntity<EtlJobMetricsSnapshot> getJobMetrics(@PathVariable String jobId) {
        log.debug("Received request to get metrics for job: {}", jobId);
        Optional<EtlJobMetricsSnapshot> snapshot = metricsCollector.getSnapshot(jobId);
        return snapshot
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Получить метрики для всех job'ов
     *
     * @return Map с метриками по всем job'ам
     */
    @GetMapping
    public ResponseEntity<Map<String, EtlJobMetricsSnapshot>> getAllMetrics() {
        log.debug("Received request to get all metrics");
        Map<String, EtlJobMetricsSnapshot> allMetrics = metricsCollector.snapshotAll();
        return ResponseEntity.ok(allMetrics);
    }

    /**
     * Получить только статус конкретного job'а
     *
     * @param jobId идентификатор job'а
     * @return статус job'а или 404 если job не найден
     */
    @GetMapping("/{jobId}/status")
    public ResponseEntity<StatusResponse> getJobStatus(@PathVariable String jobId) {
        log.debug("Received request to get status for job: {}", jobId);
        Optional<EtlJobStatus> status = metricsCollector.getStatus(jobId);
        return status
                .map(s -> ResponseEntity.ok(new StatusResponse(jobId, s)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * DTO для ответа со статусом
     */
    public record StatusResponse(String jobId, EtlJobStatus status) {
    }
}
