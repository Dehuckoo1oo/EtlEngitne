package ru.pospelov.etl.engine.steps.loader;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.loader.FastSqlLoaderConfig;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.LoadingException;
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

@Slf4j
@Component
@RequiredArgsConstructor
public class FastSqlServerLoader {

    private final DataSource dataSource;

    /**
     * Load data to SQL Server using fast bulk copy with type-safe configuration.
     */
    public void load(FastSqlLoaderConfig config, String jobId, Collection<EtlRecord> records) {
        if (records.isEmpty()) return;

        String targetTable = config.targetTable();
        List<EtlRecord> batch = records instanceof List ? (List<EtlRecord>) records : new ArrayList<>(records);
        Instant start = Instant.now();

        try {
            bulkInsertBatch(targetTable, batch, jobId);
            log.debug("Inserted {} rows into '{}'", batch.size(), targetTable);
        } catch (Exception e) {
            throw new LoadingException(
                    "Bulk insert failed",
                    jobId,
                    batch.isEmpty() ? null : batch.get(0),
                    EtlErrorSeverity.CRITICAL,
                    e
            );
        }

        Instant end = Instant.now();
        log.info("✅ Fast bulk insert into '{}' completed in {} ms ({} rows)",
                targetTable,
                Duration.between(start, end).toMillis(),
                batch.size());
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

                for (String column : batch.get(0).getAll().keySet()) {
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
