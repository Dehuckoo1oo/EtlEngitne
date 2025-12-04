package ru.pospelov.etl.engine.api.repository;

import ru.pospelov.etl.engine.model.EtlJob;

import java.util.List;
import java.util.Optional;

/**
 * Интерфейс репозитория для хранения и управления ETL job'ами.
 * Абстракция позволяет легко переключаться между различными реализациями хранилища
 * (in-memory, БД, файловая система и т.д.)
 */
public interface JobRepository {

    /**
     * Сохраняет новый job или обновляет существующий
     *
     * @param job ETL job для сохранения
     * @return сохраненный job
     */
    EtlJob save(EtlJob job);

    /**
     * Находит job по его идентификатору
     *
     * @param jobId идентификатор job'а
     * @return Optional с job'ом или пустой Optional если не найден
     */
    Optional<EtlJob> findById(String jobId);

    /**
     * Возвращает все сохраненные job'ы
     *
     * @return список всех job'ов
     */
    List<EtlJob> findAll();

    /**
     * Удаляет job по идентификатору
     *
     * @param jobId идентификатор job'а для удаления
     * @return true если job был удален, false если job не найден
     */
    boolean deleteById(String jobId);

    /**
     * Проверяет существование job'а по идентификатору
     *
     * @param jobId идентификатор job'а
     * @return true если job существует, иначе false
     */
    boolean existsById(String jobId);
}
