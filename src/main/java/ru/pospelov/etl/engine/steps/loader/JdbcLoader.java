package ru.pospelov.etl.engine.steps.loader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.loader.JdbcLoaderConfig;
import ru.pospelov.etl.engine.conversion.TypeConverter;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.exception.TypeConversionException;
import ru.pospelov.etl.engine.model.ColumnMetadata;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.model.SqlVariantValue;

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
     * <p>For sql_variant columns, uses CAST to preserve exact SQL Server types
     * (e.g., decimal(18,2), varchar(50), date) instead of unpacking to base Java types.
     * This provides type precision at the cost of some performance compared to bulk copy.
     */
    public void load(JdbcLoaderConfig config, String jobId, EtlBatch batch) {
        if (batch.getRecords().isEmpty()) return;

        String targetTable = config.targetTable();
        int batchSize = config.streamBatchSize();

        List<EtlRecord> recordList = new ArrayList<>(batch.getRecords());
        Map<String, ColumnMetadata> columnMetadata = batch.getColumnMetadata();

        // Filter out Kafka envelope fields and variant metadata fields
        Set<String> columns = recordList.getFirst().getAll().keySet().stream()
                .filter(k -> !k.startsWith("__kafka_"))
                .filter(k -> !k.startsWith("__variant_"))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        log.debug("Job '{}' loading {} records into '{}' with {} columns",
                jobId, recordList.size(), targetTable, columns.size());

        try {
            long startTime = System.currentTimeMillis();
            int totalInserted = 0;

            // Group records by sql_variant type signature for efficient batching
            Map<String, List<EtlRecord>> recordsBySignature = groupByVariantSignature(recordList, columns);

            log.debug("Job '{}' grouped {} records into {} signature groups",
                    jobId, recordList.size(), recordsBySignature.size());

            // Process each group with its specific SQL (with proper CASTs for sql_variant)
            for (Map.Entry<String, List<EtlRecord>> entry : recordsBySignature.entrySet()) {
                List<EtlRecord> groupRecords = entry.getValue();

                // Build SQL with CAST for sql_variant columns based on first record in group
                String sql = buildInsertSql(targetTable, columns, groupRecords.getFirst());

                // Process in sub-batches
                for (int from = 0; from < groupRecords.size(); from += batchSize) {
                    int to = Math.min(from + batchSize, groupRecords.size());
                    List<EtlRecord> subBatch = groupRecords.subList(from, to);

                    jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            EtlRecord rec = subBatch.get(i);
                            int index = 1;
                            for (String column : columns) {
                                Object rawValue = rec.get(column);

                                // For sql_variant, use string value directly (CAST in SQL handles conversion)
                                if (rawValue instanceof SqlVariantValue) {
                                    SqlVariantValue variant = (SqlVariantValue) rawValue;
                                    ps.setString(index++, variant.getValue());
                                } else {
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
                        }

                        @Override
                        public int getBatchSize() {
                            return subBatch.size();
                        }
                    });

                    totalInserted += subBatch.size();
                }
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
     * Group records by sql_variant type signature.
     *
     * <p>Records with identical sql_variant types can use the same INSERT SQL
     * (with same CAST clauses), allowing efficient batch processing.
     *
     * <p>Example signatures:
     * <ul>
     * <li>"" - no sql_variant columns</li>
     * <li>"variant_col1:decimal(18,2)" - one sql_variant column</li>
     * <li>"col1:int|col2:varchar(50)" - two sql_variant columns</li>
     * </ul>
     *
     * @param records all records to group
     * @param columns ordered column names
     * @return map of signature → records with that signature
     */
    private Map<String, List<EtlRecord>> groupByVariantSignature(List<EtlRecord> records, Set<String> columns) {
        Map<String, List<EtlRecord>> groups = new LinkedHashMap<>();

        for (EtlRecord record : records) {
            String signature = buildVariantSignature(record, columns);
            groups.computeIfAbsent(signature, k -> new ArrayList<>()).add(record);
        }

        return groups;
    }

    /**
     * Build type signature for sql_variant columns in a record.
     *
     * <p>Signature format: "col1:type1|col2:type2|..." (alphabetically sorted by column name)
     *
     * <p>Example:
     * <pre>
     * Record: {id=1, amount=SqlVariantValue("decimal(18,2)", "100.50"), name="test"}
     * Signature: "amount:decimal(18,2)"
     * </pre>
     *
     * @param record the record to analyze
     * @param columns ordered column names
     * @return signature string (empty if no sql_variant columns)
     */
    private String buildVariantSignature(EtlRecord record, Set<String> columns) {
        List<String> parts = new ArrayList<>();

        for (String column : columns) {
            Object value = record.get(column);
            if (value instanceof SqlVariantValue) {
                SqlVariantValue variant = (SqlVariantValue) value;
                parts.add(column + ":" + variant.getSqlType());
            }
        }

        return String.join("|", parts);
    }

    /**
     * Build INSERT SQL with CAST for sql_variant columns.
     *
     * <p>For non-variant columns, uses standard placeholder "?".
     * <p>For variant columns, uses "CAST(? AS exact_type)" to preserve SQL Server types.
     *
     * <p>Example SQL:
     * <pre>
     * INSERT INTO myTable (id, amount, description)
     * VALUES (?, CAST(? AS decimal(18,2)), CAST(? AS varchar(50)))
     * </pre>
     *
     * @param targetTable table name
     * @param columns ordered column names
     * @param sampleRecord sample record from the group (to detect sql_variant types)
     * @return INSERT SQL string
     */
    private String buildInsertSql(String targetTable, Set<String> columns, EtlRecord sampleRecord) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(targetTable).append(" (");
        sql.append(String.join(", ", columns));
        sql.append(") VALUES (");

        List<String> placeholders = new ArrayList<>();
        for (String column : columns) {
            Object value = sampleRecord.get(column);
            if (value instanceof SqlVariantValue) {
                SqlVariantValue variant = (SqlVariantValue) value;
                // Use CAST to preserve exact SQL Server type
                placeholders.add("CAST(? AS " + variant.getSqlType() + ")");
            } else {
                // Regular placeholder for non-variant columns
                placeholders.add("?");
            }
        }

        sql.append(String.join(", ", placeholders));
        sql.append(")");

        return sql.toString();
    }

    /**
     * Truncate value for logging (prevent huge log entries).
     */
    private String truncateValue(Object value) {
        String str = String.valueOf(value);
        return str.length() > 100 ? str.substring(0, 100) + "..." : str;
    }
}
