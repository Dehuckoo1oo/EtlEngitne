package ru.pospelov.etl.engine.steps.loader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.loader.JdbcLoaderConfig;
import ru.pospelov.etl.engine.conversion.TypeConverter;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlStage;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.exception.TypeConversionException;
import ru.pospelov.etl.engine.model.ColumnMetadata;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class JdbcLoader {

    private final JdbcTemplate jdbcTemplate;
    private final TypeConverter typeConverter;

    /**
     * Load data to JDBC target using type-safe configuration.
     *
     * <p>Performs type conversion using TypeConverter to ensure Avro types
     * (int for dates, long for timestamps, ByteBuffer for decimals) are converted
     * to JDBC-compatible types (LocalDate, Instant, BigDecimal) before insertion.
     *
     * <p>Also handles SqlVariantValue unpacking to base types.
     */
    public void load(JdbcLoaderConfig config, String jobId, EtlBatch batch) {
        if (batch.getRecords().isEmpty()) return;

        String targetTable = config.targetTable();
        int batchSize = config.streamBatchSize();

        List<EtlRecord> recordList = new ArrayList<>(batch.getRecords());
        Map<String, ColumnMetadata> columnMetadata = batch.getColumnMetadata();

        // Filter out Kafka envelope fields and variant metadata fields
        Set<String> columns = recordList.get(0).getAll().keySet().stream()
                .filter(k -> !k.startsWith("__kafka_"))
                .filter(k -> !k.startsWith("__variant_"))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        String columnNames = String.join(", ", columns);
        String placeholders = String.join(", ", Collections.nCopies(columns.size(), "?"));
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", targetTable, columnNames, placeholders);

        log.debug("Job '{}' loading {} records into '{}' with {} columns",
                jobId, recordList.size(), targetTable, columns.size());

        try {
            long startTime = System.currentTimeMillis();
            int totalInserted = 0;

            for (int from = 0; from < recordList.size(); from += batchSize) {
                int to = Math.min(from + batchSize, recordList.size());
                List<EtlRecord> subBatch = recordList.subList(from, to);

                jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        EtlRecord rec = subBatch.get(i);
                        int index = 1;
                        for (String column : columns) {
                            Object rawValue = rec.get(column);

                            // Get column metadata (may be null if source is not JDBC)
                            ColumnMetadata meta = columnMetadata != null ? columnMetadata.get(column) : null;

                            try {
                                // Convert Avro/Any types to JDBC-compatible types
                                Object jdbcValue = typeConverter.convertToJdbc(rawValue, meta, jobId);
                                ps.setObject(index++, jdbcValue);
                            } catch (TypeConversionException e) {
                                log.error("Type conversion failed for column '{}' in record {}: actual type={}, value={}",
                                        column,
                                        rec.getOffset(),
                                        rawValue != null ? rawValue.getClass().getName() : "null",
                                        truncateValue(rawValue));
                                throw new SQLException("Type conversion failed for column '" + column + "': " + e.getMessage(), e);
                            }
                        }
                    }

                    @Override
                    public int getBatchSize() {
                        return subBatch.size();
                    }
                });

                totalInserted += subBatch.size();
            }

            long elapsedMs = System.currentTimeMillis() - startTime;
            if (log.isDebugEnabled()) {
                log.debug("Job '{}' loaded {} records into '{}' in {}ms",
                        jobId, totalInserted, targetTable, elapsedMs);
            }
        } catch (Exception e) {
            log.error("Job '{}' failed to load records into '{}': {}",
                    jobId, targetTable, e.getMessage());
            EtlRecord failedRecord = recordList.isEmpty() ? null : recordList.get(0);
            throw new LoadingException(
                    "Failed to execute JDBC batch",
                    jobId,
                    failedRecord,
                    EtlErrorSeverity.CRITICAL,
                    e
            );
        }
    }

    /**
     * Truncate value for logging (prevent huge log entries).
     */
    private String truncateValue(Object value) {
        String str = String.valueOf(value);
        return str.length() > 100 ? str.substring(0, 100) + "..." : str;
    }
}
