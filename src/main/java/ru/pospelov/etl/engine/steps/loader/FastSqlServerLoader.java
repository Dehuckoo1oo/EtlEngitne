package ru.pospelov.etl.engine.steps.loader;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.loader.FastSqlLoaderConfig;
import ru.pospelov.etl.engine.conversion.TypeConverter;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlStage;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.exception.TypeConversionException;
import ru.pospelov.etl.engine.model.ColumnMetadata;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlBulkRecord;
import ru.pospelov.etl.engine.model.EtlRecord;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class FastSqlServerLoader {

    private final DataSource dataSource;
    private final TypeConverter typeConverter;

    /**
     * Load data to SQL Server using fast bulk copy with type-safe configuration.
     *
     * <p>Performs type conversion using TypeConverter to ensure Avro types
     * (int for dates, long for timestamps, ByteBuffer for decimals) are converted
     * to JDBC-compatible types (LocalDate, Instant, BigDecimal) before bulk copy.
     *
     * <p>Also handles SqlVariantValue unpacking to base types.
     */
    public void load(FastSqlLoaderConfig config, String jobId, EtlBatch batch) {
        if (batch.getRecords().isEmpty()) return;

        String targetTable = config.targetTable();
        List<EtlRecord> recordList = batch.getRecords() instanceof List
                ? (List<EtlRecord>) batch.getRecords()
                : new ArrayList<>(batch.getRecords());
        Instant start = Instant.now();

        try {
            // Convert values before bulk copy
            List<EtlRecord> convertedRecords = convertRecords(recordList, batch.getColumnMetadata(), jobId);
            bulkInsertBatch(targetTable, convertedRecords, jobId);
            log.debug("Inserted {} rows into '{}'", recordList.size(), targetTable);
        } catch (Exception e) {
            throw new LoadingException(
                    "Bulk insert failed",
                    jobId,
                    recordList.isEmpty() ? null : recordList.get(0),
                    EtlErrorSeverity.CRITICAL,
                    e
            );
        }

        Instant end = Instant.now();
        log.info("✅ Fast bulk insert into '{}' completed in {} ms ({} rows)",
                targetTable,
                Duration.between(start, end).toMillis(),
                recordList.size());
    }

    /**
     * Convert all record values using TypeConverter before bulk copy.
     *
     * <p>This ensures that Avro types are converted to JDBC-compatible types:
     * <ul>
     * <li>int (Avro date) → LocalDate or java.sql.Date</li>
     * <li>long (Avro timestamp) → Instant or java.sql.Timestamp</li>
     * <li>ByteBuffer (Avro decimal) → BigDecimal</li>
     * <li>String (sql_variant JSON) → unpacked base type</li>
     * </ul>
     *
     * @param records original records from batch
     * @param columnMetadata metadata from source (may be null)
     * @param jobId job identifier for error reporting
     * @return new list of records with converted values
     */
    private List<EtlRecord> convertRecords(List<EtlRecord> records, Map<String, ColumnMetadata> columnMetadata, String jobId) {
        List<EtlRecord> converted = new ArrayList<>(records.size());

        for (EtlRecord original : records) {
            EtlRecord convertedRecord = new EtlRecord(
                    original.getTimestamp(),
                    original.getSourcePartition(),
                    original.getOffset()
            );

            // Filter out __kafka_* and __variant_* fields, and convert remaining fields
            original.getAll().forEach((key, rawValue) -> {
                if (!key.startsWith("__kafka_") && !key.startsWith("__variant_")) {
                    try {
                        ColumnMetadata meta = columnMetadata != null ? columnMetadata.get(key) : null;
                        Object jdbcValue = typeConverter.convertToJdbc(rawValue, meta, jobId);
                        convertedRecord.put(key, jdbcValue);
                    } catch (TypeConversionException e) {
                        log.error("Type conversion failed for column '{}' in record {}: actual type={}, value={}",
                                key,
                                original.getOffset(),
                                rawValue != null ? rawValue.getClass().getName() : "null",
                                truncateValue(rawValue));
                        throw new LoadingException(
                                String.format("Type conversion failed for column '%s': %s", key, e.getMessage()),
                                jobId,
                                original,
                                EtlErrorSeverity.CRITICAL,
                                e
                        );
                    }
                }
            });

            converted.add(convertedRecord);
        }

        return converted;
    }

    /**
     * Truncate value for logging (prevent huge log entries).
     */
    private String truncateValue(Object value) {
        String str = String.valueOf(value);
        return str.length() > 100 ? str.substring(0, 100) + "..." : str;
    }

    private void bulkInsertBatch(String targetTable, List<EtlRecord> batch, String jobId) {
        try (Connection connection = dataSource.getConnection()) {
            SQLServerConnection sqlConn = connection.unwrap(SQLServerConnection.class);
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlConn)) {
                bulkCopy.setDestinationTableName(targetTable);

                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setBatchSize(batch.size());  // Use incoming batch size for optimal network throughput
                // TableLock=false позволяет параллельную запись из нескольких потоков
                // SQL Server будет использовать page/row locks вместо table lock
                // КРИТИЧНО для производительности при многопоточной записи!
                options.setTableLock(false);
                options.setCheckConstraints(false);
                options.setFireTriggers(false);
                options.setKeepNulls(true);
                bulkCopy.setBulkCopyOptions(options);

                // Filter out Kafka envelope fields and variant metadata fields
                Set<String> userColumns = batch.get(0).getAll().keySet().stream()
                        .filter(k -> !k.startsWith("__kafka_"))
                        .filter(k -> !k.startsWith("__variant_"))
                        .collect(Collectors.toSet());

                for (String column : userColumns) {
                    bulkCopy.addColumnMapping(column, column);
                }

                bulkCopy.writeToServer(new EtlBulkRecord(batch));
            }
        } catch (SQLException e) {
            throw new LoadingException(
                    "Bulk insert failed",
                    jobId,
                    batch.isEmpty() ? null : batch.get(0),
                    EtlErrorSeverity.CRITICAL,
                    e
            );
        }
    }
}
