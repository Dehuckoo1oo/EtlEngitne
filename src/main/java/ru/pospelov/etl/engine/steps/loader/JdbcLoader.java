package ru.pospelov.etl.engine.steps.loader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

        int batchSize = (int) job.getParamOrDefault("batchSize", 1000);
        int threadCount = (int) job.getParamOrDefault("threads", 4);

        log.info("Loading {} records into {} in batches of {} with {} threads", recordList.size(), targetTable, batchSize, threadCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int from = 0; from < recordList.size(); from += batchSize) {
            int to = Math.min(from + batchSize, recordList.size());
            List<EtlRecord> batch = new ArrayList<>(recordList.subList(from, to));
            executor.submit(() -> {
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
            });
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                log.warn("JDBC load executor did not finish in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("JDBC loading interrupted", e);
        }
    }
}
