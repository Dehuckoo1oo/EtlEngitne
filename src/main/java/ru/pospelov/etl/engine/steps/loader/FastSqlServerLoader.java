package ru.pospelov.etl.engine.steps.loader;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.model.EtlBulkRecord;
import ru.pospelov.etl.engine.model.EtlJob;
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
public class FastSqlServerLoader implements Loader {

    private final DataSource dataSource;

    @Override
    public String getType() {
        return "fast-sql";
    }

    @Override
    public void load(Collection<EtlRecord> records, EtlJob job) {
        if (records.isEmpty()) return;

        String targetTable = job.getTargetTable();
        int batchSize = (int) job.getParamOrDefault("streamBatchSize", 50000);

        List<EtlRecord> allRecords = new ArrayList<>(records);
        Instant start = Instant.now();

        try {
            // Process in batches for very large collections
            for (int from = 0; from < allRecords.size(); from += batchSize) {
                int to = Math.min(from + batchSize, allRecords.size());
                List<EtlRecord> batch = allRecords.subList(from, to);

                bulkInsertBatch(targetTable, batch, job.getJobId());
                log.debug("Inserted {} rows (batch {}/{})", batch.size(), to, allRecords.size());
            }
        } catch (Exception e) {
            throw new LoadingException(
                    "Bulk insert failed",
                    job.getJobId(),
                    allRecords.isEmpty() ? null : allRecords.get(0),
                    EtlErrorSeverity.CRITICAL,
                    e
            );
        }

        Instant end = Instant.now();
        log.info("✅ Fast bulk insert into '{}' completed in {} ms ({} rows)",
                targetTable,
                Duration.between(start, end).toMillis(),
                allRecords.size());
    }

    private void bulkInsertBatch(String targetTable, List<EtlRecord> batch, String jobId) {
        try (Connection connection = dataSource.getConnection()) {
            SQLServerConnection sqlConn = connection.unwrap(SQLServerConnection.class);
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlConn)) {
                bulkCopy.setDestinationTableName(targetTable);

                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setTableLock(true);
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
