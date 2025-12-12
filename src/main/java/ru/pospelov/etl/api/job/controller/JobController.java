package ru.pospelov.etl.api.job.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.pospelov.etl.api.job.dto.JobRequest;
import ru.pospelov.etl.api.job.dto.JobResponse;
import ru.pospelov.etl.api.job.dto.JobRunRequest;
import ru.pospelov.etl.api.job.dto.JobRunResponse;
import ru.pospelov.etl.api.job.service.JobService;

import java.util.List;

/**
 * REST контроллер для управления ETL job'ами.
 *
 * Предоставляет следующие эндпоинты:
 * - POST   /api/jobs           - создать новый job
 * - PUT    /api/jobs/{id}      - обновить существующий job
 * - GET    /api/jobs/{id}      - получить job по id
 * - GET    /api/jobs           - получить все job'ы
 * - DELETE /api/jobs/{id}      - удалить job
 * - POST   /api/jobs/{id}/run  - запустить job вручную
 */
@Slf4j
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    /**
     * Создать новый ETL job
     *
     * @param request данные для создания job'а
     * @return созданный job
     */
    @PostMapping
    public ResponseEntity<JobResponse> createJob(@RequestBody JobRequest request) {
        log.info("Received request to create job: {}", request.getId());
        JobResponse response = jobService.createJob(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * Обновить существующий ETL job
     *
     * @param id идентификатор job'а
     * @param request новые данные job'а
     * @return обновленный job
     */
    @PutMapping("/{id}")
    public ResponseEntity<JobResponse> updateJob(
            @PathVariable String id,
            @RequestBody JobRequest request
    ) {
        log.info("Received request to update job: {}", id);
        JobResponse response = jobService.updateJob(id, request);
        return ResponseEntity.ok(response);
    }

    /**
     * Получить ETL job по идентификатору
     *
     * @param id идентификатор job'а
     * @return найденный job
     */
    @GetMapping("/{id}")
    public ResponseEntity<JobResponse> getJob(@PathVariable String id) {
        log.info("Received request to get job: {}", id);
        JobResponse response = jobService.getJob(id);
        return ResponseEntity.ok(response);
    }

    /**
     * Получить список всех ETL job'ов
     *
     * @return список job'ов
     */
    @GetMapping
    public ResponseEntity<List<JobResponse>> getAllJobs() {
        log.info("Received request to get all jobs");
        List<JobResponse> responses = jobService.getAllJobs();
        return ResponseEntity.ok(responses);
    }

    /**
     * Удалить ETL job по идентификатору
     *
     * @param id идентификатор job'а
     * @return статус 204 No Content при успешном удалении
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteJob(@PathVariable String id) {
        log.info("Received request to delete job: {}", id);
        jobService.deleteJob(id);
        return ResponseEntity.noContent().build();
    }

    /**
     * Запустить ETL job вручную (без планировщика)
     *
     * @param id идентификатор job'а
     * @param runRequest опциональные параметры для переопределения
     * @return информация о запуске
     */
    @PostMapping("/{id}/run")
    public ResponseEntity<JobRunResponse> runJob(
            @PathVariable String id,
            @RequestBody(required = false) JobRunRequest runRequest
    ) {
        log.info("Received request to run job: {}", id);
        JobRunResponse response = jobService.runJob(id, runRequest);
        return ResponseEntity.accepted().body(response);
    }
}
