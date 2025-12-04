package ru.pospelov.etl.engine.api.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import ru.pospelov.etl.engine.api.entity.JobEntity;
import ru.pospelov.etl.engine.api.mapper.JobMapper;
import ru.pospelov.etl.engine.model.EtlJob;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Реализация JobRepository на основе JPA для персистентного хранения job'ов в БД.
 * Использует JpaJobRepository для работы с БД и JobMapper для конвертации между EtlJob и JobEntity.
 */
@Slf4j
@Repository
@Primary
@Transactional
public class DatabaseJobRepository implements JobRepository {

    private final JpaJobRepository jpaRepository;
    private final JobMapper jobMapper;

    public DatabaseJobRepository(JpaJobRepository jpaRepository, JobMapper jobMapper) {
        this.jpaRepository = jpaRepository;
        this.jobMapper = jobMapper;
    }

    @Override
    public EtlJob save(EtlJob job) {
        log.debug("Saving job to database: {}", job.getJobId());

        Optional<JobEntity> existingEntity = jpaRepository.findById(job.getJobId());

        JobEntity entityToSave;
        if (existingEntity.isPresent()) {
            // Обновление существующего job'а
            entityToSave = jobMapper.updateEntity(existingEntity.get(), job);
            log.debug("Updating existing job: {}", job.getJobId());
        } else {
            // Создание нового job'а
            entityToSave = jobMapper.toEntity(job);
            log.debug("Creating new job: {}", job.getJobId());
        }

        JobEntity savedEntity = jpaRepository.save(entityToSave);
        log.info("Job saved to database: {}", savedEntity.getId());

        return jobMapper.toEtlJob(savedEntity);
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<EtlJob> findById(String jobId) {
        log.debug("Finding job by id: {}", jobId);

        return jpaRepository.findById(jobId)
                .map(entity -> {
                    log.debug("Job found: {}", jobId);
                    return jobMapper.toEtlJob(entity);
                });
    }

    @Override
    @Transactional(readOnly = true)
    public List<EtlJob> findAll() {
        log.debug("Finding all jobs");

        List<EtlJob> jobs = jpaRepository.findAll().stream()
                .map(jobMapper::toEtlJob)
                .collect(Collectors.toList());

        log.debug("Found {} jobs", jobs.size());
        return jobs;
    }

    @Override
    public boolean deleteById(String jobId) {
        log.debug("Deleting job by id: {}", jobId);

        if (jpaRepository.existsById(jobId)) {
            jpaRepository.deleteById(jobId);
            log.info("Job deleted: {}", jobId);
            return true;
        }

        log.debug("Job not found for deletion: {}", jobId);
        return false;
    }

    @Override
    @Transactional(readOnly = true)
    public boolean existsById(String jobId) {
        boolean exists = jpaRepository.existsById(jobId);
        log.debug("Job exists check for {}: {}", jobId, exists);
        return exists;
    }

    /**
     * Дополнительный метод для поиска job'ов по статусу
     *
     * @param status статус job'а (ACTIVE, DISABLED, ARCHIVED)
     * @return список job'ов с указанным статусом
     */
    @Transactional(readOnly = true)
    public List<EtlJob> findByStatus(String status) {
        log.debug("Finding jobs by status: {}", status);

        List<EtlJob> jobs = jpaRepository.findByStatus(status).stream()
                .map(jobMapper::toEtlJob)
                .collect(Collectors.toList());

        log.debug("Found {} jobs with status {}", jobs.size(), status);
        return jobs;
    }

    /**
     * Дополнительный метод для поиска всех активных job'ов
     *
     * @return список активных job'ов
     */
    @Transactional(readOnly = true)
    public List<EtlJob> findAllActive() {
        log.debug("Finding all active jobs");

        List<EtlJob> jobs = jpaRepository.findAllActive().stream()
                .map(jobMapper::toEtlJob)
                .collect(Collectors.toList());

        log.debug("Found {} active jobs", jobs.size());
        return jobs;
    }
}
