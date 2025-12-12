package ru.pospelov.etl.api.repository;

import org.springframework.stereotype.Repository;
import ru.pospelov.etl.engine.model.EtlJob;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory реализация JobRepository.
 * Использует ConcurrentHashMap для thread-safe хранения job'ов в памяти.
 * Подходит для быстрого прототипирования и тестирования.
 * Данные не сохраняются при перезапуске приложения.
 */
@Repository
public class InMemoryJobRepository implements JobRepository {

    private final Map<String, EtlJob> jobs = new ConcurrentHashMap<>();

    @Override
    public EtlJob save(EtlJob job) {
        if (job == null || job.jobId() == null) {
            throw new IllegalArgumentException("Job and jobId cannot be null");
        }
        jobs.put(job.jobId(), job);
        return job;
    }

    @Override
    public Optional<EtlJob> findById(String jobId) {
        if (jobId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(jobs.get(jobId));
    }

    @Override
    public List<EtlJob> findAll() {
        return new ArrayList<>(jobs.values());
    }

    @Override
    public boolean deleteById(String jobId) {
        if (jobId == null) {
            return false;
        }
        return jobs.remove(jobId) != null;
    }

    @Override
    public boolean existsById(String jobId) {
        return jobId != null && jobs.containsKey(jobId);
    }
}
