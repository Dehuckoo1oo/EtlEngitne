package ru.pospelov.etl.engine.steps.extractor.jdbc;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.ExtractionException;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Component
@RequiredArgsConstructor
public class JdbcExtractor {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JdbcExtractor.class);

    private final JdbcTemplate jdbcTemplate;

    /**
     * Extract data from JDBC source using type-safe configuration.
     * All parameters are type-safe and NOT NULL (except Optional).
     * NO getParamOrDefault - everything is explicitly specified by user.
     */
    public void extract(
            JdbcExtractorConfig config,
            String jobId,
            Consumer<Collection<EtlRecord>> batchConsumer
    ) {
        // Type-safe field access
        String query = config.sqlQuery();
        int threads = config.threads();
        int streamBatchSize = config.streamBatchSize();
        int partitions = config.partitions();

        // Optional - explicit nullable handling
        Optional<String> partitionColumn = config.partitionColumn();
        Optional<String> keyColumn = config.keyColumn();

        log.info("Job '{}' extraction started: query={}, threads={}, batchSize={}, partitions={}",
                jobId, query, threads, streamBatchSize, partitions);

        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final List<Future<?>> tasks = new ArrayList<>();

        try {
            // Partitioning only if partition column is specified
            if (partitionColumn.isPresent() && partitions > 1) {
                log.info("Job '{}' using partition-based extraction: column={}, partitions={}",
                        jobId, partitionColumn.get(), partitions);

                for (int p = 0; p < partitions; p++) {
                    String partQuery = query +
                            (query.toLowerCase().contains("where") ? " AND " : " WHERE ") +
                            partitionColumn.get() + " = " + p;

                    tasks.add(executor.submit(new JdbcPartitionQueryTask(
                            jdbcTemplate,
                            partQuery,
                            p,
                            streamBatchSize,
                            keyColumn.orElse(""),
                            batchConsumer
                    )));
                }
            } else {
                // Offset-based extraction
                Integer totalRows = jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM (" + query + ") t",
                        Integer.class
                );
                int total = (totalRows == null) ? 0 : totalRows;

                log.info("Job '{}' using offset-based extraction: totalRows={}, tasks={}",
                        jobId, total, (total + streamBatchSize - 1) / streamBatchSize);

                for (int offset = 0; offset < total; offset += streamBatchSize) {
                    tasks.add(executor.submit(new JdbcOffsetQueryTask(
                            jdbcTemplate,
                            query,
                            offset,
                            streamBatchSize,
                            keyColumn.orElse(""),
                            batchConsumer
                    )));
                }
            }

            waitForTasks(tasks, jobId);
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                throw new ExtractionException("JdbcExtractor did not finish within timeout", jobId);
            }
            log.info("Job '{}' extraction completed: {} tasks finished", jobId, tasks.size());
        } catch (EtlException e) {
            log.error("Job '{}' extraction failed: {}", jobId, e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Job '{}' extraction interrupted", jobId);
            throw new ExtractionException("JdbcExtractor interrupted", jobId, null, EtlErrorSeverity.CRITICAL, e);
        } catch (Exception e) {
            log.error("Job '{}' extraction error: {}", jobId, e.getMessage(), e);
            throw new ExtractionException("JdbcExtractor failed", jobId, null, EtlErrorSeverity.CRITICAL, e);
        } finally {
            executor.shutdownNow();
        }
    }

    private static void waitForTasks(List<Future<?>> tasks, String jobId) {
        for (Future<?> task : tasks) {
            try {
                task.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ExtractionException("JdbcExtractor interrupted", jobId, null, EtlErrorSeverity.CRITICAL, e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof EtlException etlException) {
                    throw etlException;
                }
                throw new ExtractionException("JdbcExtractor task failed", jobId, null, EtlErrorSeverity.CRITICAL, cause);
            }
        }
    }
}
