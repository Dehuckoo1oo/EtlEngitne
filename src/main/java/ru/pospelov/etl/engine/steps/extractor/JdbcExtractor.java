package ru.pospelov.etl.engine.steps.extractor;

import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.ExtractionException;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
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

    private final JdbcTemplate jdbcTemplate;

    @Override
    public String getType() {
        return "sql";
    }

    @Override
    public void extract(EtlJob job, Consumer<Collection<EtlRecord>> batchConsumer) {
        String query = job.getSourceQuery();
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

        Schema avroSchema = null;
        if (schemaStr != null) {
            avroSchema = new Schema.Parser().parse(schemaStr);
        }

        final Map<String, String> avroFieldLookup = buildAvroFieldLookup(avroSchema);
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final List<Future<?>> tasks = new ArrayList<>();

        try {
            if (!partitionColumn.isEmpty() && partitionCount > 1) {
                for (int p = 0; p < partitionCount; p++) {
                    final int part = p;
                    final String partQuery = query +
                            (query.toLowerCase().contains("where") ? " AND " : " WHERE ") +
                            partitionColumn + " = " + part;

                    tasks.add(executor.submit(new PartitionQueryTask(
                            jdbcTemplate,
                            partQuery,
                            part,
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

                for (int offset = 0; offset < total; offset += streamBatchSize) {
                    final int off = offset;

                    tasks.add(executor.submit(new OffsetQueryTask(
                            jdbcTemplate,
                            query,
                            off,
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
        } catch (EtlException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ExtractionException("JdbcExtractor interrupted", job.getJobId(), null, EtlErrorSeverity.CRITICAL, e);
        } catch (Exception e) {
            throw new ExtractionException("JdbcExtractor failed", job.getJobId(), null, EtlErrorSeverity.CRITICAL, e);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Задача для выборки по партициям (partitionColumn).
     */
    private static class PartitionQueryTask implements Runnable {

        private final JdbcTemplate jdbcTemplate;
        private final String query;
        private final int part;
        private final int streamBatchSize;
        private final Schema avroSchema;
        private final Map<String, String> avroFieldLookup;
        private final String keyColumn;
        private final Consumer<Collection<EtlRecord>> batchConsumer;

        PartitionQueryTask(JdbcTemplate jdbcTemplate,
                           String query,
                           int part,
                           int streamBatchSize,
                           Schema avroSchema,
                           Map<String, String> avroFieldLookup,
                           String keyColumn,
                           Consumer<Collection<EtlRecord>> batchConsumer) {
            this.jdbcTemplate = jdbcTemplate;
            this.query = query;
            this.part = part;
            this.streamBatchSize = streamBatchSize;
            this.avroSchema = avroSchema;
            this.avroFieldLookup = avroFieldLookup;
            this.keyColumn = keyColumn;
            this.batchConsumer = batchConsumer;
        }

        @Override
        public void run() {
            ResultSetExtractor<Void> extractor = new StreamingResultSetExtractor(
                    "sql-part-" + part,
                    streamBatchSize,
                    avroSchema,
                    avroFieldLookup,
                    keyColumn,
                    batchConsumer
            );
            jdbcTemplate.query(query, extractor);
        }
    }

    /**
     * Задача для выборки по offset/batchSize.
     */
    private static class OffsetQueryTask implements Runnable {

        private final JdbcTemplate jdbcTemplate;
        private final String baseQuery;
        private final int offset;
        private final int streamBatchSize;
        private final Schema avroSchema;
        private final Map<String, String> avroFieldLookup;
        private final String keyColumn;
        private final Consumer<Collection<EtlRecord>> batchConsumer;

        OffsetQueryTask(JdbcTemplate jdbcTemplate,
                        String baseQuery,
                        int offset,
                        int streamBatchSize,
                        Schema avroSchema,
                        Map<String, String> avroFieldLookup,
                        String keyColumn,
                        Consumer<Collection<EtlRecord>> batchConsumer) {
            this.jdbcTemplate = jdbcTemplate;
            this.baseQuery = baseQuery;
            this.offset = offset;
            this.streamBatchSize = streamBatchSize;
            this.avroSchema = avroSchema;
            this.avroFieldLookup = avroFieldLookup;
            this.keyColumn = keyColumn;
            this.batchConsumer = batchConsumer;
        }

        @Override
        public void run() {
            String pagedQuery = baseQuery +
                    " OFFSET " + offset + " ROWS FETCH NEXT " + streamBatchSize + " ROWS ONLY";

            ResultSetExtractor<Void> extractor = new StreamingResultSetExtractor(
                    "sql",
                    streamBatchSize,
                    avroSchema,
                    avroFieldLookup,
                    keyColumn,
                    batchConsumer
            );
            jdbcTemplate.query(pagedQuery, extractor);
        }
    }

    /**
     * Streaming ResultSetExtractor that processes records in batches.
     * Instead of accumulating all records in memory, it invokes the consumer
     * for each batch of records.
     */
    private static class StreamingResultSetExtractor implements ResultSetExtractor<Void> {

        private final String sourcePartition;
        private final int batchSize;
        private final Schema avroSchema;
        private final Map<String, String> avroFieldLookup;
        private final String keyColumn;
        private final Consumer<Collection<EtlRecord>> batchConsumer;

        StreamingResultSetExtractor(String sourcePartition,
                                    int batchSize,
                                    Schema avroSchema,
                                    Map<String, String> avroFieldLookup,
                                    String keyColumn,
                                    Consumer<Collection<EtlRecord>> batchConsumer) {
            this.sourcePartition = sourcePartition;
            this.batchSize = batchSize;
            this.avroSchema = avroSchema;
            this.avroFieldLookup = avroFieldLookup;
            this.keyColumn = keyColumn;
            this.batchConsumer = batchConsumer;
        }

        @Override
        public Void extractData(ResultSet rs) throws SQLException {
            ResultSetMetaData md = rs.getMetaData();
            List<EtlRecord> currentBatch = new ArrayList<>(batchSize);

            while (rs.next()) {
                GenericRecord avro = (avroSchema != null) ? new GenericData.Record(avroSchema) : null;

                EtlRecord record = new EtlRecord(Instant.now(), sourcePartition, rs.getRow());
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    String col = md.getColumnLabel(i);
                    Object val = rs.getObject(i);
                    String avroFieldName = resolveFieldName(avroFieldLookup, col);
                    if (avro != null && avroFieldName != null) {
                        avro.put(avroFieldName, val);
                    } else {
                        record.put(col, val);
                    }
                }
                if (!keyColumn.isEmpty()) {
                    record.put("key", rs.getObject(keyColumn));
                }
                if (avro != null) {
                    record.put("value", avro);
                }
                currentBatch.add(record);

                // When batch is full, send it for processing immediately
                if (currentBatch.size() >= batchSize) {
                    batchConsumer.accept(new ArrayList<>(currentBatch));
                    currentBatch.clear();
                }
            }

            // Send remaining records
            if (!currentBatch.isEmpty()) {
                batchConsumer.accept(currentBatch);
            }

            return null;
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

    private static String resolveFieldName(Map<String, String> lookup, String columnLabel) {
        if (lookup.isEmpty() || columnLabel == null) {
            return null;
        }
        String exact = lookup.get(columnLabel);
        if (exact != null) {
            return exact;
        }
        return lookup.get(columnLabel.toLowerCase(Locale.ROOT));
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
