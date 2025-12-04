package ru.pospelov.etl.engine.steps.loader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

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

        int batchSize = (int) job.getParamOrDefault("streamBatchSize", 50000);

        log.debug("Job '{}' loading {} records into '{}' with {} columns",
                job.getJobId(), recordList.size(), targetTable, columns.size());

        try {
            long startTime = System.currentTimeMillis();
            int totalInserted = 0;

            for (int from = 0; from < recordList.size(); from += batchSize) {
                int to = Math.min(from + batchSize, recordList.size());
                List<EtlRecord> batch = recordList.subList(from, to);

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

                totalInserted += batch.size();
            }

            long elapsedMs = System.currentTimeMillis() - startTime;
            if (log.isDebugEnabled()) {
                log.debug("Job '{}' loaded {} records into '{}' in {}ms",
                        job.getJobId(), totalInserted, targetTable, elapsedMs);
            }
        } catch (Exception e) {
            log.error("Job '{}' failed to load records into '{}': {}",
                    job.getJobId(), targetTable, e.getMessage());
            EtlRecord failedRecord = recordList.isEmpty() ? null : recordList.get(0);
            throw new LoadingException(
                    "Failed to execute JDBC batch",
                    job.getJobId(),
                    failedRecord,
                    EtlErrorSeverity.CRITICAL,
                    e
            );
        }
    }
}
