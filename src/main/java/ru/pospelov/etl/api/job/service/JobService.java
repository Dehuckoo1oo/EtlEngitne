package ru.pospelov.etl.api.job.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.pospelov.etl.api.job.dto.JobRequest;
import ru.pospelov.etl.api.job.dto.JobResponse;
import ru.pospelov.etl.api.job.dto.JobRunRequest;
import ru.pospelov.etl.api.job.dto.JobRunResponse;
import ru.pospelov.etl.api.job.exception.JobAlreadyExistsException;
import ru.pospelov.etl.api.job.exception.JobAlreadyRunningException;
import ru.pospelov.etl.api.job.exception.JobExecutionException;
import ru.pospelov.etl.api.job.exception.JobNotFoundException;
import ru.pospelov.etl.api.job.repository.JobRepository;
import ru.pospelov.etl.engine.config.KafkaFormat;
import ru.pospelov.etl.engine.config.extractor.ExtractorConfig;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.config.extractor.KafkaExtractorConfig;
import ru.pospelov.etl.engine.config.loader.FastSqlLoaderConfig;
import ru.pospelov.etl.engine.config.loader.JdbcLoaderConfig;
import ru.pospelov.etl.engine.config.loader.KafkaLoaderConfig;
import ru.pospelov.etl.engine.config.loader.LoaderConfig;
import ru.pospelov.etl.engine.config.transformer.AvroToRecordTransformerConfig;
import ru.pospelov.etl.engine.config.transformer.NoopTransformerConfig;
import ru.pospelov.etl.engine.config.transformer.RecordToAvroTransformerConfig;
import ru.pospelov.etl.engine.config.transformer.TransformerConfig;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.pipeline.EtlPipeline;
import ru.pospelov.etl.engine.pipeline.EtlPipelineFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Service for managing ETL jobs.
 * Provides business logic for CRUD operations and job execution.
 *
 * NOTE: This service bridges between the old DTO format (Map-based params)
 * and the new type-safe configuration model. This will be fully replaced in Phase 7.
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
     * Create a new job
     */
    public JobResponse createJob(JobRequest request) {
        validateJobRequest(request);

        if (jobRepository.existsById(request.getId())) {
            throw new JobAlreadyExistsException(request.getId());
        }

        EtlJob job = toEtlJob(request);
        EtlJob savedJob = jobRepository.save(job);

        log.info("Created job with id: {}", savedJob.jobId());
        return toJobResponse(savedJob);
    }

    /**
     * Update existing job
     */
    public JobResponse updateJob(String jobId, JobRequest request) {
        validateJobRequest(request);

        if (!jobRepository.existsById(jobId)) {
            throw new JobNotFoundException(jobId);
        }

        if (!jobId.equals(request.getId())) {
            throw new IllegalArgumentException("Job id in path (%s) doesn't match id in request body (%s)"
                    .formatted(jobId, request.getId()));
        }

        EtlJob job = toEtlJob(request);
        EtlJob savedJob = jobRepository.save(job);

        log.info("Updated job with id: {}", savedJob.jobId());
        return toJobResponse(savedJob);
    }

    /**
     * Get job by id
     */
    public JobResponse getJob(String jobId) {
        EtlJob job = jobRepository.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));
        return toJobResponse(job);
    }

    /**
     * Get all jobs
     */
    public List<JobResponse> getAllJobs() {
        return jobRepository.findAll().stream()
                .map(this::toJobResponse)
                .collect(Collectors.toList());
    }

    /**
     * Delete job by id
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
     * Run job manually (without scheduler)
     */
    public JobRunResponse runJob(String jobId, JobRunRequest runRequest) {
        EtlJob job = jobRepository.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));

        // Check if job is already running
        checkJobNotRunning(jobId);

        // Note: Runtime parameter merging is not supported with type-safe configs
        // All configuration must be specified when creating/updating the job
        if (runRequest != null && runRequest.getParams() != null && !runRequest.getParams().isEmpty()) {
            log.warn("Runtime parameters are not supported with type-safe configuration. Ignoring: {}",
                    runRequest.getParams().keySet());
        }

        long startTime = Instant.now().toEpochMilli();

        // Run job asynchronously using dedicated executor
        CompletableFuture.runAsync(() -> {
            try {
                log.info("Starting execution of job: {}", jobId);
                logJobConfiguration(job);
                EtlPipeline pipeline = pipelineFactory.createStreamingEtlPipeline();
                pipeline.run(job);
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
     * Check if job is already running
     */
    private void checkJobNotRunning(String jobId) {
        metricsCollector.getStatus(jobId).ifPresent(status -> {
            if (isRunningStatus(status)) {
                log.warn("Attempt to run job '{}' that is already running with status: {}", jobId, status);
                throw new JobAlreadyRunningException(jobId, status);
            }
        });
    }

    /**
     * Check if status is "running"
     */
    private boolean isRunningStatus(EtlJobStatus status) {
        return status == EtlJobStatus.RUNNING ||
               status == EtlJobStatus.EXTRACTING ||
               status == EtlJobStatus.TRANSFORMING ||
               status == EtlJobStatus.LOADING;
    }

    /**
     * Log complete job configuration before execution
     */
    private void logJobConfiguration(EtlJob job) {
        log.info("========================================");
        log.info("Job Configuration for: {}", job.jobId());
        log.info("========================================");
        log.info("Extractor: {} - {}", job.extractorConfig().type(), job.extractorConfig());
        log.info("Transformer: {} - {}", job.transformerConfig().type(), job.transformerConfig());
        log.info("Loader: {} - {}", job.loaderConfig().type(), job.loaderConfig());
        log.info("========================================");
    }

    /**
     * Validate job request
     */
    private void validateJobRequest(JobRequest request) {
        if (request.getId() == null || request.getId().trim().isEmpty()) {
            throw new IllegalArgumentException("Job id is required");
        }

        if (request.getParams() == null || request.getParams().isEmpty()) {
            throw new IllegalArgumentException("Job params are required");
        }

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
     * Convert JobRequest to EtlJob (bridge between old DTO format and new type-safe model)
     */
    private EtlJob toEtlJob(JobRequest request) {
        Map<String, Object> params = request.getParams();

        String extractorType = (String) params.get("extractorType");
        String transformerType = (String) params.get("transformerType");
        String loaderType = (String) params.get("loaderType");

        ExtractorConfig extractorConfig = createExtractorConfig(extractorType, params, request.getSource());
        TransformerConfig transformerConfig = createTransformerConfig(transformerType, params);
        LoaderConfig loaderConfig = createLoaderConfig(loaderType, params, request.getTarget());

        return new EtlJob(request.getId(), extractorConfig, transformerConfig, loaderConfig);
    }

    /**
     * Create ExtractorConfig from old-style params
     */
    private ExtractorConfig createExtractorConfig(String type, Map<String, Object> params, String source) {
        return switch (type) {
            case "sql" -> {
                // Support both sqlQuery (custom query) and table (table-based mode)
                String sqlQuery = source != null ? source : (String) params.get("sqlQuery");
                String table = (String) params.get("table");

                Optional<String> sqlQueryOpt = Optional.ofNullable(sqlQuery);
                Optional<String> tableOpt = Optional.ofNullable(table);
                Optional<String> partitionColumn = Optional.ofNullable((String) params.get("partitionColumn"));
                int partitions = getIntParam(params, "partitions", 1);
                Optional<String> keyColumn = Optional.ofNullable((String) params.get("keyColumn"));
                int threads = getIntParam(params, "threads", 1);
                int streamBatchSize = getIntParam(params, "streamBatchSize", 1000);

                yield new JdbcExtractorConfig(sqlQueryOpt, tableOpt, partitionColumn, partitions, keyColumn, threads, streamBatchSize);
            }
            case "kafka" -> {
                String topic = source != null ? source : (String) params.get("topic");
                long startTimestamp = getLongParam(params, "startTimestamp", 0L);
                long endTimestamp = getLongParam(params, "endTimestamp", Long.MAX_VALUE);
                String formatStr = (String) params.getOrDefault("format", "AVRO");
                KafkaFormat format = KafkaFormat.valueOf(formatStr.toUpperCase());
                int threads = getIntParam(params, "threads", 1);
                int streamBatchSize = getIntParam(params, "streamBatchSize", 1000);

                yield new KafkaExtractorConfig(topic, startTimestamp, endTimestamp, format, threads, streamBatchSize);
            }
            default -> throw new IllegalArgumentException("Unknown extractor type: " + type);
        };
    }

    /**
     * Create TransformerConfig from old-style params
     */
    private TransformerConfig createTransformerConfig(String type, Map<String, Object> params) {
        return switch (type) {
            case "noop" -> new NoopTransformerConfig();
            case "avro" -> new AvroToRecordTransformerConfig();
            case "record-to-avro" -> {
                String avroSchemaSubject = (String) params.get("avroSchema");
                if (avroSchemaSubject == null) {
                    avroSchemaSubject = (String) params.get("avroSchemaSubject");
                }
                yield new RecordToAvroTransformerConfig(avroSchemaSubject);
            }
            default -> throw new IllegalArgumentException("Unknown transformer type: " + type);
        };
    }

    /**
     * Create LoaderConfig from old-style params
     */
    private LoaderConfig createLoaderConfig(String type, Map<String, Object> params, String target) {
        return switch (type) {
            case "jdbc" -> {
                String targetTable = target != null ? target : (String) params.get("targetTable");
                int streamBatchSize = getIntParam(params, "streamBatchSize", 1000);
                yield new JdbcLoaderConfig(targetTable, streamBatchSize);
            }
            case "fast-sql" -> {
                String targetTable = target != null ? target : (String) params.get("targetTable");
                yield new FastSqlLoaderConfig(targetTable);
            }
            case "kafka" -> {
                String topic = target != null ? target : (String) params.get("topic");
                String formatStr = (String) params.getOrDefault("format", "AVRO");
                KafkaFormat format = KafkaFormat.valueOf(formatStr.toUpperCase());
                yield new KafkaLoaderConfig(topic, format);
            }
            default -> throw new IllegalArgumentException("Unknown loader type: " + type);
        };
    }

    /**
     * Convert EtlJob to JobResponse (bridge between new type-safe model and old DTO format)
     */
    private JobResponse toJobResponse(EtlJob job) {
        Map<String, Object> params = new HashMap<>();

        // Add extractor params
        params.put("extractorType", job.extractorConfig().type());
        addExtractorParams(params, job.extractorConfig());

        // Add transformer params
        params.put("transformerType", job.transformerConfig().type());
        addTransformerParams(params, job.transformerConfig());

        // Add loader params
        params.put("loaderType", job.loaderConfig().type());
        addLoaderParams(params, job.loaderConfig());

        // Extract source and target for backward compatibility
        String source = extractSource(job.extractorConfig());
        String target = extractTarget(job.loaderConfig());

        return JobResponse.builder()
                .id(job.jobId())
                .source(source)
                .target(target)
                .params(params)
                .build();
    }

    private void addExtractorParams(Map<String, Object> params, ExtractorConfig config) {
        switch (config) {
            case JdbcExtractorConfig jdbc -> {
                params.put("sqlQuery", jdbc.sqlQuery());
                jdbc.partitionColumn().ifPresent(pc -> params.put("partitionColumn", pc));
                params.put("partitions", jdbc.partitions());
                jdbc.keyColumn().ifPresent(kc -> params.put("keyColumn", kc));
                params.put("threads", jdbc.threads());
                params.put("streamBatchSize", jdbc.streamBatchSize());
            }
            case KafkaExtractorConfig kafka -> {
                params.put("topic", kafka.topic());
                params.put("startTimestamp", kafka.startTimestamp());
                params.put("endTimestamp", kafka.endTimestamp());
                params.put("format", kafka.format().name());
                params.put("threads", kafka.threads());
                params.put("streamBatchSize", kafka.streamBatchSize());
            }
        }
    }

    private void addTransformerParams(Map<String, Object> params, TransformerConfig config) {
        switch (config) {
            case NoopTransformerConfig noop -> {
                // No additional params
            }
            case AvroToRecordTransformerConfig avro -> {
                // No additional params
            }
            case RecordToAvroTransformerConfig recordToAvro -> {
                params.put("avroSchemaSubject", recordToAvro.avroSchemaSubject());
            }
        }
    }

    private void addLoaderParams(Map<String, Object> params, LoaderConfig config) {
        switch (config) {
            case JdbcLoaderConfig jdbc -> {
                params.put("targetTable", jdbc.targetTable());
                params.put("streamBatchSize", jdbc.streamBatchSize());
            }
            case FastSqlLoaderConfig fastSql -> {
                params.put("targetTable", fastSql.targetTable());
            }
            case KafkaLoaderConfig kafka -> {
                params.put("topic", kafka.topic());
                params.put("format", kafka.format().name());
            }
        }
    }

    private String extractSource(ExtractorConfig config) {
        return switch (config) {
            case JdbcExtractorConfig jdbc -> jdbc.sqlQuery().orElse(jdbc.table().orElse(""));
            case KafkaExtractorConfig kafka -> kafka.topic();
        };
    }

    private String extractTarget(LoaderConfig config) {
        return switch (config) {
            case JdbcLoaderConfig jdbc -> jdbc.targetTable();
            case FastSqlLoaderConfig fastSql -> fastSql.targetTable();
            case KafkaLoaderConfig kafka -> kafka.topic();
        };
    }

    private int getIntParam(Map<String, Object> params, String key, int defaultValue) {
        Object value = params.get(key);
        if (value == null) return defaultValue;
        if (value instanceof Number) return ((Number) value).intValue();
        return Integer.parseInt(value.toString());
    }

    private long getLongParam(Map<String, Object> params, String key, long defaultValue) {
        Object value = params.get(key);
        if (value == null) return defaultValue;
        if (value instanceof Number) return ((Number) value).longValue();
        return Long.parseLong(value.toString());
    }
}
