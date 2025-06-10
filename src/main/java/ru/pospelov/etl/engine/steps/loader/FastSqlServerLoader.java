package ru.pospelov.etl.engine.steps.loader;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        int batchSize = (int) job.getParamOrDefault("batchSize", 1000);
        int threadCount = (int) job.getParamOrDefault("threads", 4);

        List<EtlRecord> allRecords = new ArrayList<>(records);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Instant start = Instant.now();

        for (int from = 0; from < allRecords.size(); from += batchSize) {
            int to = Math.min(from + batchSize, allRecords.size());
            List<EtlRecord> batch = new ArrayList<>(allRecords.subList(from, to));

            executor.submit(() -> {
                try (Connection connection = dataSource.getConnection()) {
                    SQLServerConnection sqlConn = connection.unwrap(SQLServerConnection.class);
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlConn);
                    bulkCopy.setDestinationTableName(targetTable);

                    SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                    options.setTableLock(true);
                    options.setCheckConstraints(false);
                    options.setFireTriggers(false);
                    options.setKeepNulls(true);
                    bulkCopy.setBulkCopyOptions(options);

                    for (String column : batch.getFirst().getAll().keySet()) {
                        bulkCopy.addColumnMapping(column, column);
                    }

                    bulkCopy.writeToServer(new EtlBulkRecord(batch));
                    log.info("Thread {} inserted {} rows", Thread.currentThread().getName(), batch.size());

                } catch (SQLException e) {
                    log.error("Bulk insert failed in thread {}", Thread.currentThread().getName(), e);
                    throw new RuntimeException(e);
                }
            });
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                log.warn("Fast SQL loader did not finish in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("FastSqlServerLoader interrupted", e);
        }

        Instant end = Instant.now();
        log.info("✅ Fast bulk insert into '{}' completed in {} ms ({} rows, {} threads)",
                targetTable,
                Duration.between(start, end).toMillis(),
                allRecords.size(),
                threadCount);
    }
}
