package ru.pospelov.etl.engine.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Глобальные настройки ETL двигателя. Основные параметры:
 * - число доступных физических ядер (для расчета пулов потоков);
 * - параметры пула потоков для выполнения ETL;
 * - запас подключений для JPA/HTTP.
 */
@Configuration
@ConfigurationProperties(prefix = "etl")
@Data
public class EtlProperties {

    /**
     * Количество доступных физических ядер (без HyperThreading).
     */
    private int availableProcessors = 4;

    /**
     * Настройки пула потоков для ETL задач.
     */
    private EtlPoolProperties pool = new EtlPoolProperties();

    /**
     * Дополнительные подключения для JPA/HTTP сверх ETL пулов.
     */
    private int hikariExtraConnections = 8;

    // ========= Вспомогательные вычисления =========

    /**
     * @return минимальный размер пула: availableProcessors * coreMultiplier
     */
    public int getCorePoolSize() {
        return Math.max(1, (int) (availableProcessors * pool.getCoreMultiplier()));
    }

    /**
     * @return максимальный размер пула: availableProcessors * maxMultiplier
     */
    public int getMaxPoolSize() {
        return Math.max(1, (int) (availableProcessors * pool.getMaxMultiplier()));
    }

    /**
     * @return расчетный максимум пула HikariCP: getMaxPoolSize() + hikariExtraConnections
     */
    public int getHikariMaxPoolSize() {
        return getMaxPoolSize() + hikariExtraConnections;
    }
}
