package ru.pospelov.etl.engine.api.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.pospelov.etl.engine.api.dto.JobRequest;
import ru.pospelov.etl.engine.api.dto.JobResponse;
import ru.pospelov.etl.engine.api.dto.JobRunRequest;
import ru.pospelov.etl.engine.api.dto.JobRunResponse;
import ru.pospelov.etl.engine.api.exception.JobAlreadyExistsException;
import ru.pospelov.etl.engine.api.exception.JobAlreadyRunningException;
import ru.pospelov.etl.engine.api.exception.JobExecutionException;
import ru.pospelov.etl.engine.api.exception.JobNotFoundException;
import ru.pospelov.etl.engine.api.repository.JobRepository;
import ru.pospelov.etl.engine.engine.EtlPipeline;
import ru.pospelov.etl.engine.engine.EtlPipelineFactory;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;
import ru.pospelov.etl.engine.model.EtlJob;

import org.springframework.beans.factory.annotation.Qualifier;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Сервис для управления ETL job'ами.
 * Предоставляет бизнес-логику для CRUD операций и запуска job'ов.
 */
@Slf4j
@Service
public class JobService {

    private final JobRepository jobRepository;
    private final EtlPipelineFactory pipelineFactory;
    private final ExecutorService etlJobExecutor;
    private final EtlMetricsCollector metricsCollector;

    public JobService(
            JobRepository jobRepository,
            EtlPipelineFactory pipelineFactory,
            @Qualifier("etlJobExecutor") ExecutorService etlJobExecutor,
            EtlMetricsCollector metricsCollector) {
        this.jobRepository = jobRepository;
        this.pipelineFactory = pipelineFactory;
        this.etlJobExecutor = etlJobExecutor;
        this.metricsCollector = metricsCollector;
    }

    /**
     * Создает новый job
     */
    public JobResponse createJob(JobRequest request) {
        validateJobRequest(request);

        if (jobRepository.existsById(request.getId())) {
            throw new JobAlreadyExistsException(request.getId());
        }

        EtlJob job = toEtlJob(request);
        EtlJob savedJob = jobRepository.save(job);

        log.info("Created job with id: {}", savedJob.getJobId());
        return toJobResponse(savedJob);
    }

    /**
     * Обновляет существующий job
     */
    public JobResponse updateJob(String jobId, JobRequest request) {
        validateJobRequest(request);

        if (!jobRepository.existsById(jobId)) {
            throw new JobNotFoundException(jobId);
        }

        // Ensure the id in request matches the path parameter
        if (!jobId.equals(request.getId())) {
            throw new IllegalArgumentException("Job id in path (%s) doesn't match id in request body (%s)"
                    .formatted(jobId, request.getId()));
        }

        EtlJob job = toEtlJob(request);
        EtlJob savedJob = jobRepository.save(job);

        log.info("Updated job with id: {}", savedJob.getJobId());
        return toJobResponse(savedJob);
    }

    /**
     * Получает job по id
     */
    public JobResponse getJob(String jobId) {
        EtlJob job = jobRepository.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));
        return toJobResponse(job);
    }

    /**
     * Получает все job'ы
     */
    public List<JobResponse> getAllJobs() {
        return jobRepository.findAll().stream()
                .map(this::toJobResponse)
                .collect(Collectors.toList());
    }

    /**
     * Удаляет job по id
     */
    public void deleteJob(String jobId) {
        if (!jobRepository.existsById(jobId)) {
            throw new JobNotFoundException(jobId);
        }

        boolean deleted = jobRepository.deleteById(jobId);
        if (deleted) {
            log.info("Deleted job with id: {}", jobId);
        }
    }

    /**
     * Запускает job вручную (без планировщика)
     */
    public JobRunResponse runJob(String jobId, JobRunRequest runRequest) {
        EtlJob job = jobRepository.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));

        // Проверяем, не выполняется ли уже этот job
        checkJobNotRunning(jobId);

        // Merge runtime parameters with job parameters
        EtlJob jobWithRuntimeParams = mergeParameters(job, runRequest);

        long startTime = Instant.now().toEpochMilli();

        // Run job asynchronously using dedicated executor (NOT ForkJoinPool.commonPool!)
        CompletableFuture.runAsync(() -> {
            try {
                log.info("Starting execution of job: {}", jobId);
                logJobConfiguration(jobWithRuntimeParams);
                EtlPipeline pipeline = pipelineFactory.create(jobWithRuntimeParams);
                pipeline.run(jobWithRuntimeParams);
                log.info("Job {} completed successfully", jobId);
            } catch (Exception e) {
                log.error("Job {} failed with error: {}", jobId, e.getMessage(), e);
                throw new JobExecutionException(jobId, e);
            }
        }, etlJobExecutor);

        return JobRunResponse.builder()
                .jobId(jobId)
                .status("STARTED")
                .message("Job execution started successfully")
                .startedAt(startTime)
                .build();
    }

    /**
     * Проверяет, не выполняется ли уже job.
     * Выбрасывает JobAlreadyRunningException если job уже в процессе выполнения.
     */
    private void checkJobNotRunning(String jobId) {
        metricsCollector.getStatus(jobId).ifPresent(status -> {
            // Проверяем, является ли статус "выполняющимся"
            if (isRunningStatus(status)) {
                log.warn("Attempt to run job '{}' that is already running with status: {}", jobId, status);
                throw new JobAlreadyRunningException(jobId, status);
            }
        });
    }

    /**
     * Проверяет, является ли статус "выполняющимся"
     */
    private boolean isRunningStatus(EtlJobStatus status) {
        return status == EtlJobStatus.RUNNING ||
               status == EtlJobStatus.EXTRACTING ||
               status == EtlJobStatus.TRANSFORMING ||
               status == EtlJobStatus.LOADING;
    }

    /**
     * Объединяет параметры job'а с параметрами запуска
     */
    private EtlJob mergeParameters(EtlJob job, JobRunRequest runRequest) {
        if (runRequest == null || runRequest.getParams() == null || runRequest.getParams().isEmpty()) {
            return job;
        }

        Map<String, Object> mergedParams = new HashMap<>(job.getParameters());
        mergedParams.putAll(runRequest.getParams());

        return new EtlJob(
                job.getJobId(),
                job.getSource(),
                job.getTargetTable(),
                mergedParams
        );
    }

    /**
     * Логирует полную конфигурацию job'а перед запуском
     */
    private void logJobConfiguration(EtlJob job) {
        log.info("========================================");
        log.info("Job Configuration for: {}", job.getJobId());
        log.info("========================================");
        log.info("Source: {}", job.getSource());
        log.info("Target: {}", job.getTargetTable());
        log.info("Parameters:");

        Map<String, Object> params = job.getParameters();

        // Основные типы компонентов
        log.info("  extractorType: {}", params.get("extractorType"));
        log.info("  transformerType: {}", params.get("transformerType"));
        log.info("  loaderType: {}", params.get("loaderType"));

        // Параметры производительности
        log.info("  threads: {}", params.get("threads"));
        log.info("  streamBatchSize: {}", params.get("streamBatchSize"));

        // Параметры партиционирования
        log.info("  partitionColumn: {}", params.get("partitionColumn"));
        log.info("  partitions: {}", params.get("partitions"));

        // Kafka-специфичные параметры
        log.info("  topic: {}", params.get("topic"));
        log.info("  format: {}", params.get("format"));
        log.info("  avroSchema: {}", params.get("avroSchema"));
        log.info("  keyColumn: {}", params.get("keyColumn"));
        log.info("  startTimestamp: {}", params.get("startTimestamp"));
        log.info("  endTimestamp: {}", params.get("endTimestamp"));

        // Все остальные параметры
        log.info("All parameters:");
        params.forEach((key, value) -> {
            if (value != null) {
                log.info("  {} = {} ({})", key, value, value.getClass().getSimpleName());
            }
        });
        log.info("========================================");
    }

    /**
     * Валидирует запрос создания/обновления job'а
     */
    private void validateJobRequest(JobRequest request) {
        if (request.getId() == null || request.getId().trim().isEmpty()) {
            throw new IllegalArgumentException("Job id is required");
        }

        if (request.getParams() == null || request.getParams().isEmpty()) {
            throw new IllegalArgumentException("Job params are required");
        }

        // Validate required parameters
        String extractorType = (String) request.getParams().get("extractorType");
        String transformerType = (String) request.getParams().get("transformerType");
        String loaderType = (String) request.getParams().get("loaderType");

        if (extractorType == null || extractorType.trim().isEmpty()) {
            throw new IllegalArgumentException("extractorType parameter is required");
        }

        if (transformerType == null || transformerType.trim().isEmpty()) {
            throw new IllegalArgumentException("transformerType parameter is required");
        }

        if (loaderType == null || loaderType.trim().isEmpty()) {
            throw new IllegalArgumentException("loaderType parameter is required");
        }
    }

    /**
     * Конвертирует JobRequest в EtlJob
     */
    private EtlJob toEtlJob(JobRequest request) {
        return new EtlJob(
                request.getId(),
                request.getSource(),
                request.getTarget(),
                request.getParams()
        );
    }

    /**
     * Конвертирует EtlJob в JobResponse
     */
    private JobResponse toJobResponse(EtlJob job) {
        return JobResponse.builder()
                .id(job.getJobId())
                .source(job.getSource())
                .target(job.getTargetTable())
                .params(job.getParameters())
                .build();
    }
}
