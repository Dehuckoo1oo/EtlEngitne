package ru.pospelov.etl.engine.steps.extractor;

import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.ExtractionException;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.schema.SchemaRegistryService;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Component
@RequiredArgsConstructor
public class JdbcExtractor implements Extractor {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JdbcExtractor.class);

    private final JdbcTemplate jdbcTemplate;
    private final SchemaRegistryService schemaRegistryService;

    @Override
    public String getType() {
        return "sql";
    }

    @Override
    public void extract(EtlJob job, Consumer<Collection<EtlRecord>> batchConsumer) {
        String query = job.getSource();
        if (query == null) {
            query = Objects.toString(job.getParam("query"), "");
        }
        if (query.isBlank()) {
            throw new ExtractionException("Source query is required for JdbcExtractor", job.getJobId());
        }

        int streamBatchSize = (int) job.getParamOrDefault("streamBatchSize", 50000);
        int threadCount = (int) job.getParamOrDefault("threads", 4);
        int partitionCount = (int) job.getParamOrDefault("partitions", 1);
        String partitionColumn = Objects.toString(job.getParam("partitionColumn"), "");
        String schemaStr = (String) job.getParam("avroSchema");
        String keyColumn = Objects.toString(job.getParamOrDefault("keyColumn", ""), "");

        log.info("Job '{}' extraction started: batchSize={}, threads={}, partitions={}",
                job.getJobId(), streamBatchSize, threadCount, partitionCount);

        Schema avroSchema = null;
        if (schemaStr != null) {
            if (schemaStr.trim().startsWith("{")) {
                avroSchema = new Schema.Parser().parse(schemaStr);
            } else {
                try {
                    avroSchema = schemaRegistryService.getLatestSchema(schemaStr);
                } catch (Exception e) {
                    throw new ExtractionException("Failed to fetch schema from Schema Registry for subject: " + schemaStr, job.getJobId(), e);
                }
            }
        }

        final Map<String, String> avroFieldLookup = buildAvroFieldLookup(avroSchema);
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final List<Future<?>> tasks = new ArrayList<>();

        try {
            if (!partitionColumn.isEmpty() && partitionCount > 1) {
                log.info("Job '{}' using partition-based extraction: column={}, partitions={}",
                        job.getJobId(), partitionColumn, partitionCount);
                for (int p = 0; p < partitionCount; p++) {
                    final String partQuery = query +
                            (query.toLowerCase().contains("where") ? " AND " : " WHERE ") +
                            partitionColumn + " = " + p;

                    tasks.add(executor.submit(new JdbcPartitionQueryTask(
                            jdbcTemplate,
                            partQuery,
                            p,
                            streamBatchSize,
                            avroSchema,
                            avroFieldLookup,
                            keyColumn,
                            batchConsumer
                    )));
                }
            } else {
                Integer totalRows = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM (" + query + ") t", Integer.class);
                final int total = (totalRows == null) ? 0 : totalRows;
                log.info("Job '{}' using offset-based extraction: totalRows={}, tasks={}",
                        job.getJobId(), total, (total + streamBatchSize - 1) / streamBatchSize);

                for (int offset = 0; offset < total; offset += streamBatchSize) {
                    tasks.add(executor.submit(new JdbcOffsetQueryTask(
                            jdbcTemplate,
                            query,
                            offset,
                            streamBatchSize,
                            avroSchema,
                            avroFieldLookup,
                            keyColumn,
                            batchConsumer
                    )));
                }
            }

            waitForTasks(tasks, job.getJobId());
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                throw new ExtractionException("JdbcExtractor did not finish within timeout", job.getJobId());
            }
            log.info("Job '{}' extraction completed: {} tasks finished", job.getJobId(), tasks.size());
        } catch (EtlException e) {
            log.error("Job '{}' extraction failed: {}", job.getJobId(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Job '{}' extraction interrupted", job.getJobId());
            throw new ExtractionException("JdbcExtractor interrupted", job.getJobId(), null, EtlErrorSeverity.CRITICAL, e);
        } catch (Exception e) {
            log.error("Job '{}' extraction error: {}", job.getJobId(), e.getMessage(), e);
            throw new ExtractionException("JdbcExtractor failed", job.getJobId(), null, EtlErrorSeverity.CRITICAL, e);
        } finally {
            executor.shutdownNow();
        }
    }

    private static Map<String, String> buildAvroFieldLookup(Schema avroSchema) {
        if (avroSchema == null) {
            return Collections.emptyMap();
        }
        Map<String, String> lookup = new HashMap<>();
        for (Schema.Field field : avroSchema.getFields()) {
            lookup.put(field.name(), field.name());
            lookup.put(field.name().toLowerCase(Locale.ROOT), field.name());
        }
        return Collections.unmodifiableMap(lookup);
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
