package ru.pospelov.etl.engine.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Централизованная конфигурация ETL движка.
 * Все размеры пулов потоков и соединений рассчитываются на основе availableProcessors.
 * <p>
 * ВАЖНО: Различие между потоками и соединениями:
 * - ThreadPool (потоки): работают на CPU, maxPoolSize = количество физических ядер
 * - HikariCP (соединения): TCP соединения к БД, не занимают CPU постоянно
 * <p>
 * Пример для сервера с 12 физическими ядрами:
 * - ThreadPool maxPoolSize = 12 потоков (используют все 12 ядер CPU)
 * - HikariCP maxPoolSize = 20 соединений (12 для ETL + 8 для JPA/HTTP)
 * - CPU занято: 12 потоков ✅
 * - Соединения к БД: 20 (но одновременно активных ~12-15) ✅
 */
@Configuration
@ConfigurationProperties(prefix = "etl")
@Data
public class EtlProperties {

    /**
     * Количество доступных CPU ядер (ФИЗИЧЕСКИХ, без HyperThreading).
     * ВАЖНО: должно быть явно указано в application.yml!
     * <p>
     * Примеры:
     * - Dev-машина: 4 (часть CPU занята Docker)
     * - Сервер: 12 (24 потока = 12 физических ядер с HT)
     * <p>
     * Дефолт 4 — безопасное значение для dev-машины.
     */
    private int availableProcessors = 4;

    /**
     * Настройки пула потоков
     */
    private Pool pool = new Pool();

    /**
     * Фиксированный запас соединений для JPA/HTTP помимо ETL потоков.
     * <p>
     * Эти соединения используются для:
     * - REST API endpoints (Spring MVC/WebFlux)
     * - Spring Data JPA repositories
     * - Background задачи (метрики, healthchecks, scheduled tasks)
     * <p>
     * ВАЖНО: Это количество СОЕДИНЕНИЙ к БД, а не потоков!
     * Соединения почти не занимают CPU, это просто открытые TCP сокеты.
     */
    private int hikariExtraConnections = 8;

    @Data
    public static class Pool {
        /**
         * Коэффициент для расчёта corePoolSize.
         * corePoolSize = availableProcessors * coreMultiplier
         */
        private double coreMultiplier = 0.5;

        /**
         * Коэффициент для расчёта maxPoolSize.
         * maxPoolSize = availableProcessors * maxMultiplier
         * <p>
         * НЕ больше количества ФИЗИЧЕСКИХ ядер (без HT), чтобы не переподписать CPU!
         * Значение 1.0 означает: 1 поток на 1 физическое ядро (оптимально для CPU-bound задач)
         */
        private double maxMultiplier = 1.0;

        /**
         * Размер очереди задач
         */
        private int queueSize = 500;

        /**
         * Keep-alive время для idle потоков (секунды)
         */
        private int keepAliveSeconds = 120;
    }

    // ========== Вычисляемые значения ==========

    /**
     * Базовый размер пула потоков (количество потоков, которые всегда живы).
     * <p>
     * Эти потоки работают на CPU!
     *
     * @return availableProcessors * coreMultiplier
     */
    public int getCorePoolSize() {
        return Math.max(1, (int) (availableProcessors * pool.getCoreMultiplier()));
    }

    /**
     * Максимальный размер пула потоков.
     * НЕ должен превышать количество физических ядер!
     * <p>
     * Эти потоки работают на CPU!
     *
     * @return availableProcessors * maxMultiplier
     */
    public int getMaxPoolSize() {
        return Math.max(1, (int) (availableProcessors * pool.getMaxMultiplier()));
    }

    /**
     * Максимальный размер пула соединений HikariCP.
     * <p>
     * ВАЖНО: Это количество СОЕДИНЕНИЙ к БД, а не потоков!
     * Формула: maxPoolSize (ETL потоки) + hikariExtraConnections (JPA/HTTP)
     * <p>
     * Пример для сервера с 12 физическими ядрами:
     * - maxPoolSize = 12 потоков (используют все 12 ядер CPU)
     * - hikariExtraConnections = 8 соединений (для JPA/HTTP)
     * - hikariMaxPoolSize = 20 соединений к БД
     * <p>
     * CPU занято: только 12 потоков на 12 ядрах ✅
     * Соединения: 20 открытых TCP к SQL Server (не занимают CPU постоянно) ✅
     *
     * @return maxPoolSize + hikariExtraConnections
     */
    public int getHikariMaxPoolSize() {
        return getMaxPoolSize() + hikariExtraConnections;
    }
}
