package ru.pospelov.etl.engine.steps.loader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class JdbcLoader implements Loader {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public String getType() {
        return "sql";
    }

    @Override
    public void load(Collection<EtlRecord> records, EtlJob job) {
        if (records.isEmpty()) return;

        String targetTable = job.getTargetTable();
        List<EtlRecord> recordList = new ArrayList<>(records);
        Set<String> columns = recordList.get(0).getAll().keySet();

        String columnNames = String.join(", ", columns);
        String placeholders = String.join(", ", Collections.nCopies(columns.size(), "?"));
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", targetTable, columnNames, placeholders);
        log.info("Executing SQL: {}", sql);
        int batchSize = (int) job.getParamOrDefault("batchSize", 1000);
        int threadCount = (int) job.getParamOrDefault("threads", 4);

        log.info("Loading {} records into {} in batches of {} with {} threads", recordList.size(), targetTable, batchSize, threadCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        for (int from = 0; from < recordList.size(); from += batchSize) {
            int to = Math.min(from + batchSize, recordList.size());
            List<EtlRecord> batch = new ArrayList<>(recordList.subList(from, to));
            futures.add(executor.submit(() -> {
                try {
                    jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            EtlRecord rec = batch.get(i);
                            int index = 1;
                            for (String column : columns) {
                                ps.setObject(index++, rec.get(column));
                            }
                        }

                        @Override
                        public int getBatchSize() {
                            return batch.size();
                        }
                    });
                } catch (Exception e) {
                    EtlRecord failedRecord = batch.isEmpty() ? null : batch.get(0);
                    throw new LoadingException(
                            "Failed to execute JDBC batch",
                            job.getJobId(),
                            failedRecord,
                            EtlErrorSeverity.CRITICAL,
                            e
                    );
                }
            }));
        }

        try {
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new LoadingException("JDBC loading interrupted", job.getJobId(), null, EtlErrorSeverity.CRITICAL, e);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof EtlException etlException) {
                        throw etlException;
                    }
                    throw new LoadingException("JDBC loading failed", job.getJobId(), null, EtlErrorSeverity.CRITICAL, cause);
                }
            }

            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                throw new LoadingException("JDBC load executor did not finish in time", job.getJobId(), null);
            }
        } catch (EtlException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LoadingException("JDBC loading interrupted", job.getJobId(), null, EtlErrorSeverity.CRITICAL, e);
        } finally {
            executor.shutdownNow();
        }
    }
}
