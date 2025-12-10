package ru.pospelov.etl.engine.config;

import lombok.Data;

/**
 * Настройки пула потоков для ETL задач.
 */
@Data
public class EtlPoolProperties {

    /**
     * Коэффициент для расчета corePoolSize: availableProcessors * coreMultiplier.
     */
    private double coreMultiplier = 0.5;

    /**
     * Коэффициент для расчета maxPoolSize: availableProcessors * maxMultiplier.
     */
    private double maxMultiplier = 1.0;

    /**
     * Размер очереди задач.
     */
    private int queueSize = 500;

    /**
     * Idle timeout в секундах.
     */
    private int keepAliveSeconds = 120;
}
