package ru.pospelov.etl.engine.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.pospelov.etl.engine.model.EtlBulkRecord;
import ru.pospelov.etl.engine.model.EtlRecord;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Конфигурация для прогрева JVM и критических компонентов при старте приложения.
 * 
 * Проблема: При первом запуске ETL job после старта приложения производительность
 * значительно ниже (~70K rows/sec) по сравнению с тестами (~110K rows/sec).
 * 
 * Причина: JIT компилятор ещё не оптимизировал критический путь:
 * - JDBC операции
 * - SQLServerBulkCopy
 * - Создание EtlRecord объектов
 * - HikariCP connection acquisition
 * 
 * Решение: При старте приложения выполняем "прогревочные" операции,
 * которые заставляют JIT скомпилировать критический код.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class WarmupConfig {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    /**
     * Выполняет прогрев после полной инициализации приложения.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void warmupOnStartup() {
        log.info("Starting JVM warmup for ETL components...");
        long start = System.currentTimeMillis();

        try {
            // 1. Прогрев HikariCP connection pool - получаем и возвращаем соединения
            warmupConnectionPool();

            // 2. Прогрев JDBC операций
            warmupJdbcOperations();

            // 3. Прогрев EtlRecord и bulk operations (в памяти, без БД)
            warmupEtlRecordOperations();

            // 4. Прогрев многопоточности
            warmupThreadPool();

            long elapsed = System.currentTimeMillis() - start;
            log.info("✅ JVM warmup completed in {} ms", elapsed);

        } catch (Exception e) {
            log.warn("JVM warmup partially failed (non-critical): {}", e.getMessage());
        }
    }

    /**
     * Прогревает HikariCP - открывает и закрывает соединения несколько раз.
     */
    private void warmupConnectionPool() {
        log.debug("Warming up connection pool...");
        for (int i = 0; i < 10; i++) {
            try (Connection conn = dataSource.getConnection()) {
                // Выполняем простой запрос для активации соединения
                try (var stmt = conn.createStatement()) {
                    stmt.execute("SELECT 1");
                }
            } catch (Exception e) {
                log.debug("Connection warmup iteration {} failed: {}", i, e.getMessage());
            }
        }
    }

    /**
     * Прогревает JdbcTemplate и SQL операции.
     */
    private void warmupJdbcOperations() {
        log.debug("Warming up JDBC operations...");
        for (int i = 0; i < 10; i++) {
            try {
                jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            } catch (Exception e) {
                log.debug("JDBC warmup iteration {} failed: {}", i, e.getMessage());
            }
        }
    }

    /**
     * Прогревает создание EtlRecord объектов и bulk operations.
     * Это критически важно, так как EtlRecord и EtlBulkRecord используются
     * миллионы раз во время ETL операций.
     */
    private void warmupEtlRecordOperations() {
        log.debug("Warming up EtlRecord operations...");
        
        // Создаём много EtlRecord объектов для прогрева JIT
        for (int iteration = 0; iteration < 5; iteration++) {
            List<EtlRecord> records = new ArrayList<>(1000);
            Instant now = Instant.now();
            
            for (int i = 0; i < 1000; i++) {
                EtlRecord record = new EtlRecord(now, "warmup", i);
                record.put("field1", "value" + i);
                record.put("field2", i * 100L);
                record.put("field3", i * 1.5);
                records.add(record);
            }
            
            // Прогреваем EtlBulkRecord (ISQLServerBulkData implementation)
            try {
                EtlBulkRecord bulkRecord = new EtlBulkRecord(records);
                // Итерируем для прогрева next() и getters
                while (bulkRecord.next()) {
                    for (int col : bulkRecord.getColumnOrdinals()) {
                        bulkRecord.getColumnName(col);
                        bulkRecord.getColumnType(col);
                    }
                    bulkRecord.getRowData();
                }
            } catch (Exception e) {
                log.debug("EtlBulkRecord warmup failed: {}", e.getMessage());
            }
        }
    }

    /**
     * Прогревает thread pool executor - создаёт потоки и выполняет задачи.
     */
    private void warmupThreadPool() {
        log.debug("Warming up thread pool...");
        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Runnable> tasks = new ArrayList<>();
        
        for (int i = 0; i < 16; i++) {
            final int taskId = i;
            tasks.add(() -> {
                // Простая вычислительная задача для прогрева
                long sum = 0;
                for (int j = 0; j < 10000; j++) {
                    sum += j * taskId;
                }
                // Prevent dead code elimination
                if (sum < 0) log.trace("Warmup sum: {}", sum);
            });
        }
        
        tasks.forEach(executor::submit);
        executor.shutdown();
        
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

