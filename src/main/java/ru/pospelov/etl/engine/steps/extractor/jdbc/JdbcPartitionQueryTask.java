package ru.pospelov.etl.engine.steps.extractor.jdbc;

import org.springframework.jdbc.core.JdbcTemplate;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Задача выборки для отдельной партиции (partitionColumn).
 * Выделена из JdbcExtractor для ясности связки компонентов.
 */
final class JdbcPartitionQueryTask implements Runnable {

    private final JdbcTemplate jdbcTemplate;
    private final String query;
    private final int part;
    private final int streamBatchSize;
    private final String keyColumn;
    private final Consumer<Collection<EtlRecord>> batchConsumer;

    JdbcPartitionQueryTask(JdbcTemplate jdbcTemplate,
                           String query,
                           int part,
                           int streamBatchSize,
                           String keyColumn,
                           Consumer<Collection<EtlRecord>> batchConsumer) {
        this.jdbcTemplate = jdbcTemplate;
        this.query = query;
        this.part = part;
        this.streamBatchSize = streamBatchSize;
        this.keyColumn = keyColumn;
        this.batchConsumer = batchConsumer;
    }

    @Override
    public void run() {
        JdbcStreamingResultSetExtractor extractor = new JdbcStreamingResultSetExtractor(
                "sql-part-" + part,
                streamBatchSize,
                keyColumn,
                batchConsumer
        );
        jdbcTemplate.query(query, extractor);
    }
}
