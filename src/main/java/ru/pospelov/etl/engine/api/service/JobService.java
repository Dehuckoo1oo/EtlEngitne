package ru.pospelov.etl.engine.api.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.pospelov.etl.engine.api.dto.JobRequest;
import ru.pospelov.etl.engine.api.dto.JobResponse;
import ru.pospelov.etl.engine.api.dto.JobRunRequest;
import ru.pospelov.etl.engine.api.dto.JobRunResponse;
import ru.pospelov.etl.engine.api.exception.JobAlreadyExistsException;
import ru.pospelov.etl.engine.api.exception.JobExecutionException;
import ru.pospelov.etl.engine.api.exception.JobNotFoundException;
import ru.pospelov.etl.engine.api.repository.JobRepository;
import ru.pospelov.etl.engine.engine.EtlPipeline;
import ru.pospelov.etl.engine.engine.EtlPipelineFactory;
import ru.pospelov.etl.engine.model.EtlJob;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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

    public JobService(JobRepository jobRepository, EtlPipelineFactory pipelineFactory) {
        this.jobRepository = jobRepository;
        this.pipelineFactory = pipelineFactory;
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

        // Merge runtime parameters with job parameters
        EtlJob jobWithRuntimeParams = mergeParameters(job, runRequest);

        long startTime = Instant.now().toEpochMilli();

        // Run job asynchronously to avoid blocking the API response
        CompletableFuture.runAsync(() -> {
            try {
                log.info("Starting execution of job: {}", jobId);
                EtlPipeline pipeline = pipelineFactory.create(jobWithRuntimeParams);
                pipeline.run(jobWithRuntimeParams);
                log.info("Job {} completed successfully", jobId);
            } catch (Exception e) {
                log.error("Job {} failed with error: {}", jobId, e.getMessage(), e);
                throw new JobExecutionException(jobId, e);
            }
        });

        return JobRunResponse.builder()
                .jobId(jobId)
                .status("STARTED")
                .message("Job execution started successfully")
                .startedAt(startTime)
                .build();
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
                job.getSourceQuery(),
                job.getTargetTable(),
                mergedParams
        );
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
                request.getSourceQuery(),
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
                .sourceQuery(job.getSourceQuery())
                .target(job.getTargetTable())
                .params(job.getParameters())
                .build();
    }
}
