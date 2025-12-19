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
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.exception.TypeConversionException;
import ru.pospelov.etl.engine.model.ColumnMetadata;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlBulkRecord;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.model.SqlVariantValue;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
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
     * Load data to SQL Server using fast bulk copy with type-safe conversion.
     */
    public void load(FastSqlLoaderConfig config, String jobId, EtlBatch batch) {
        if (batch.getRecords().isEmpty()) return;

        String targetTable = config.targetTable();
        List<EtlRecord> recordList = batch.getRecords() instanceof List
                ? (List<EtlRecord>) batch.getRecords()
                : new ArrayList<>(batch.getRecords());
        Instant start = Instant.now();

        try {
            List<EtlRecord> convertedRecords = convertRecords(recordList, batch.getColumnMetadata(), jobId);
            Map<String, List<EtlRecord>> grouped = groupByVariantSignature(convertedRecords);
            for (List<EtlRecord> groupRecords : grouped.values()) {
                bulkInsertBatch(targetTable, groupRecords, batch.getColumnMetadata(), jobId);
            }
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
        log.info("Fast bulk insert into '{}' completed in {} ms ({} rows)",
                targetTable,
                Duration.between(start, end).toMillis(),
                recordList.size());
    }

    /**
     * Convert all record values before bulk copy.
     */
    private List<EtlRecord> convertRecords(List<EtlRecord> records, Map<String, ColumnMetadata> columnMetadata, String jobId) {
        List<EtlRecord> converted = new ArrayList<>(records.size());

        for (EtlRecord original : records) {
            EtlRecord convertedRecord = new EtlRecord(
                    original.getTimestamp(),
                    original.getSourcePartition(),
                    original.getOffset()
            );

            original.getAll().forEach((key, rawValue) -> {
                if (!key.startsWith("__kafka_") && !key.startsWith("__variant_")) {
                    try {
                        ColumnMetadata meta = columnMetadata != null ? columnMetadata.get(key) : null;
                        Object jdbcValue = rawValue instanceof SqlVariantValue
                                ? rawValue
                                : typeConverter.convertToJdbc(rawValue, meta, jobId);

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

    private Map<String, List<EtlRecord>> groupByVariantSignature(List<EtlRecord> records) {
        Map<String, List<EtlRecord>> groups = new LinkedHashMap<>();

        for (EtlRecord record : records) {
            String signature = buildVariantSignature(record);
            groups.computeIfAbsent(signature, k -> new ArrayList<>()).add(record);
        }

        return groups;
    }

    private String buildVariantSignature(EtlRecord record) {
        List<String> parts = new ArrayList<>();

        record.getAll().forEach((key, value) -> {
            if (value instanceof SqlVariantValue) {
                SqlVariantValue variant = (SqlVariantValue) value;
                parts.add(key + ":" + variant.getSqlType());
            }
        });

        if (parts.isEmpty()) {
            return "";
        }

        Collections.sort(parts);
        return String.join("|", parts);
    }

    /**
     * Truncate value for logging (prevent huge log entries).
     */
    private String truncateValue(Object value) {
        String str = String.valueOf(value);
        return str.length() > 100 ? str.substring(0, 100) + "..." : str;
    }

    private void bulkInsertBatch(String targetTable, List<EtlRecord> records, Map<String, ColumnMetadata> columnMetadata, String jobId) {
        try (Connection connection = dataSource.getConnection()) {
            SQLServerConnection sqlConn = connection.unwrap(SQLServerConnection.class);
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlConn)) {
                bulkCopy.setDestinationTableName(targetTable);

                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setBatchSize(records.size());
                options.setTableLock(false);
                options.setCheckConstraints(false);
                options.setFireTriggers(false);
                options.setKeepNulls(true);
                bulkCopy.setBulkCopyOptions(options);

                Set<String> userColumns = records.get(0).getAll().keySet().stream()
                        .filter(k -> !k.startsWith("__kafka_"))
                        .filter(k -> !k.startsWith("__variant_"))
                        .collect(Collectors.toSet());

                for (String column : userColumns) {
                    bulkCopy.addColumnMapping(column, column);
                }

                bulkCopy.writeToServer(new EtlBulkRecord(records, columnMetadata));
            }
        } catch (SQLException e) {
            log.error("Bulk insert failed with SQL error: {} (SQLState: {}, ErrorCode: {})",
                    e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
            throw new LoadingException(
                    "Bulk insert failed: " + e.getMessage(),
                    jobId,
                    records.isEmpty() ? null : records.get(0),
                    EtlErrorSeverity.CRITICAL,
                    e
            );
        }
    }
}
