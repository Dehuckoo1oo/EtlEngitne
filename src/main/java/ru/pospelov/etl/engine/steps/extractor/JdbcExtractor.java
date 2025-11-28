package ru.pospelov.etl.engine.steps.extractor;

import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;
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

@Component
@RequiredArgsConstructor
public class JdbcExtractor implements Extractor {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public String getType() {
        return "sql";
    }

    @Override
    public Collection<EtlRecord> extract(EtlJob job) {
        String query = job.getSourceQuery();
        if (query == null) {
            query = Objects.toString(job.getParam("query"), "");
        }
        if (query.isEmpty()) {
            throw new IllegalArgumentException("Source query is required for JdbcExtractor");
        }

        int batchSize = (int) job.getParamOrDefault("batchSize", 1000);
        int threadCount = (int) job.getParamOrDefault("threads", 4);
        int partitionCount = (int) job.getParamOrDefault("partitions", 1);
        String partitionColumn = Objects.toString(job.getParam("partitionColumn"), "");
        String schemaStr = (String) job.getParam("avroSchema");
        String keyColumn = Objects.toString(job.getParamOrDefault("keyColumn", ""), "");

        Schema avroSchema = null;
        if (schemaStr != null) {
            avroSchema = new Schema.Parser().parse(schemaStr);
        }

        final List<EtlRecord> result = Collections.synchronizedList(new ArrayList<EtlRecord>());
        final Map<String, String> avroFieldLookup = buildAvroFieldLookup(avroSchema);
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final List<Future<?>> tasks = new ArrayList<>();

        if (!partitionColumn.isEmpty() && partitionCount > 1) {
            // Партиционированный режим
            for (int p = 0; p < partitionCount; p++) {
                final int part = p;
                final String partQuery = query +
                        (query.toLowerCase().contains("where") ? " AND " : " WHERE ") +
                        partitionColumn + " = " + part;

                tasks.add(executor.submit(new PartitionQueryTask(
                        jdbcTemplate,
                        partQuery,
                        part,
                        avroSchema,
                        avroFieldLookup,
                        keyColumn,
                        result
                )));
            }
        } else {
            // Постаточный режим (offset / fetch next)
            Integer totalRows = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM (" + query + ") t", Integer.class);
            final int total = (totalRows == null) ? 0 : totalRows;

            for (int offset = 0; offset < total; offset += batchSize) {
                final int off = offset;

                tasks.add(executor.submit(new OffsetQueryTask(
                        jdbcTemplate,
                        query,
                        off,
                        batchSize,
                        avroSchema,
                        avroFieldLookup,
                        keyColumn,
                        result
                )));
            }
        }

        waitForTasks(tasks);
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * Задача для выборки по партициям (partitionColumn).
     */
    private static class PartitionQueryTask implements Runnable {

        private final JdbcTemplate jdbcTemplate;
        private final String query;
        private final int part;
        private final Schema avroSchema;
        private final Map<String, String> avroFieldLookup;
        private final String keyColumn;
        private final List<EtlRecord> result;

        PartitionQueryTask(JdbcTemplate jdbcTemplate,
                           String query,
                           int part,
                           Schema avroSchema,
                           Map<String, String> avroFieldLookup,
                           String keyColumn,
                           List<EtlRecord> result) {
            this.jdbcTemplate = jdbcTemplate;
            this.query = query;
            this.part = part;
            this.avroSchema = avroSchema;
            this.avroFieldLookup = avroFieldLookup;
            this.keyColumn = keyColumn;
            this.result = result;
        }

        @Override
        public void run() {
            ResultSetExtractor<Void> extractor = new PartitionResultSetExtractor(
                    part,
                    avroSchema,
                    avroFieldLookup,
                    keyColumn,
                    result
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
        private final int batchSize;
        private final Schema avroSchema;
        private final Map<String, String> avroFieldLookup;
        private final String keyColumn;
        private final List<EtlRecord> result;

        OffsetQueryTask(JdbcTemplate jdbcTemplate,
                        String baseQuery,
                        int offset,
                        int batchSize,
                        Schema avroSchema,
                        Map<String, String> avroFieldLookup,
                        String keyColumn,
                        List<EtlRecord> result) {
            this.jdbcTemplate = jdbcTemplate;
            this.baseQuery = baseQuery;
            this.offset = offset;
            this.batchSize = batchSize;
            this.avroSchema = avroSchema;
            this.avroFieldLookup = avroFieldLookup;
            this.keyColumn = keyColumn;
            this.result = result;
        }

        @Override
        public void run() {
            String pagedQuery = baseQuery +
                    " OFFSET " + offset + " ROWS FETCH NEXT " + batchSize + " ROWS ONLY";

            ResultSetExtractor<Void> extractor = new OffsetResultSetExtractor(
                    offset,
                    avroSchema,
                    avroFieldLookup,
                    keyColumn,
                    result
            );
            jdbcTemplate.query(pagedQuery, extractor);
        }
    }

    /**
     * ResultSetExtractor для партиционированного запроса.
     */
    private static class PartitionResultSetExtractor implements ResultSetExtractor<Void> {

        private final int part;
        private final Schema avroSchema;
        private final Map<String, String> avroFieldLookup;
        private final String keyColumn;
        private final List<EtlRecord> result;

        PartitionResultSetExtractor(int part,
                                    Schema avroSchema,
                                    Map<String, String> avroFieldLookup,
                                    String keyColumn,
                                    List<EtlRecord> result) {
            this.part = part;
            this.avroSchema = avroSchema;
            this.avroFieldLookup = avroFieldLookup;
            this.keyColumn = keyColumn;
            this.result = result;
        }

        @Override
        public Void extractData(ResultSet rs) throws SQLException {
            ResultSetMetaData md = rs.getMetaData();

            while (rs.next()) {
                GenericRecord avro = (avroSchema != null) ? new GenericData.Record(avroSchema) : null;

                EtlRecord record = new EtlRecord(Instant.now(), "sql-part-" + part, rs.getRow());
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
                result.add(record);
            }
            return null;
        }
    }

    /**
     * ResultSetExtractor для запросов с offset/batch.
     */
    private static class OffsetResultSetExtractor implements ResultSetExtractor<Void> {

        private final int offset;
        private final Schema avroSchema;
        private final Map<String, String> avroFieldLookup;
        private final String keyColumn;
        private final List<EtlRecord> result;

        OffsetResultSetExtractor(int offset,
                                 Schema avroSchema,
                                 Map<String, String> avroFieldLookup,
                                 String keyColumn,
                                 List<EtlRecord> result) {
            this.offset = offset;
            this.avroSchema = avroSchema;
            this.avroFieldLookup = avroFieldLookup;
            this.keyColumn = keyColumn;
            this.result = result;
        }

        @Override
        public Void extractData(ResultSet rs) throws SQLException {
            ResultSetMetaData md = rs.getMetaData();

            while (rs.next()) {
                GenericRecord avro = (avroSchema != null) ? new GenericData.Record(avroSchema) : null;

                EtlRecord record = new EtlRecord(Instant.now(), "sql", offset + rs.getRow());
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
                result.add(record);
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

    private static void waitForTasks(List<Future<?>> tasks) {
        for (Future<?> task : tasks) {
            try {
                task.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("JdbcExtractor interrupted", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("JdbcExtractor task failed", e.getCause());
            }
        }
    }
}
