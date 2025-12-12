package ru.pospelov.etl.api.job.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import ru.pospelov.etl.api.job.entity.JobEntity;

import java.util.List;

/**
 * JPA-репозиторий для работы с JobEntity.
 * Предоставляет стандартные CRUD операции и дополнительные методы запросов.
 */
@Repository
public interface JpaJobRepository extends JpaRepository<JobEntity, String> {

    /**
     * Находит все job'ы по статусу
     *
     * @param status статус job'а (ACTIVE, DISABLED, ARCHIVED)
     * @return список job'ов с указанным статусом
     */
    List<JobEntity> findByStatus(String status);

    /**
     * Проверяет существование job'а по id и статусу
     *
     * @param id     идентификатор job'а
     * @param status статус job'а
     * @return true если job существует с указанным статусом
     */
    boolean existsByIdAndStatus(String id, String status);

    /**
     * Находит все активные job'ы
     *
     * @return список активных job'ов
     */
    @Query("SELECT j FROM JobEntity j WHERE j.status = 'ACTIVE' ORDER BY j.createdAt DESC")
    List<JobEntity> findAllActive();
}