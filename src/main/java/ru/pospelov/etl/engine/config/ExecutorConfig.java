package ru.pospelov.etl.engine.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Конфигурация пулов потоков для ETL операций.
 * <p>
 * Использование выделенного пула вместо ForkJoinPool.commonPool()
 * позволяет избежать contention и обеспечивает стабильную производительность.
 * <p>
 * Размеры пула рассчитываются автоматически на основе EtlProperties:
 * - corePoolSize = availableProcessors * coreMultiplier
 * - maxPoolSize = availableProcessors * maxMultiplier
 * <p>
 * Один общий пул для всех ETL операций (extraction, transformation, loading)
 * обеспечивает суммарную конкуренцию не выше физических ядер CPU.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class ExecutorConfig {

    private final EtlProperties etlProperties;

    /**
     * Выделенный ExecutorService для запуска ETL job'ов.
     * <p>
     * Параметры рассчитываются динамически на основе конфигурации:
     * - corePoolSize: базовое количество потоков (всегда живы)
     * - maxPoolSize: максимум потоков (не больше физических ядер!)
     * - keepAliveTime: освобождаем idle потоки
     * - queue: буфер для ожидающих job'ов
     * - CallerRunsPolicy: при переполнении - выполняем в вызывающем потоке
     * <p>
     * Примеры значений:
     * - Dev (4 CPU): core=2, max=4, queue=500
     * - Server (12 CPU): core=6, max=12, queue=500
     */
    @Bean(name = "etlJobExecutor")
    public ExecutorService etlJobExecutor() {
        int corePoolSize = etlProperties.getCorePoolSize();
        int maxPoolSize = etlProperties.getMaxPoolSize();
        int keepAliveSeconds = etlProperties.getPool().getKeepAliveSeconds();
        int queueSize = etlProperties.getPool().getQueueSize();

        log.info("Creating ETL ThreadPoolExecutor: core={}, max={}, queue={}, keepAlive={}s",
                corePoolSize, maxPoolSize, queueSize, keepAliveSeconds);

        return new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveSeconds, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueSize),
                r -> {
                    Thread t = new Thread(r);
                    t.setName("etl-job-" + t.getId());
                    t.setDaemon(false);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }
}
