package ru.pospelov.etl.engine.steps.extractor.jdbc;

import org.springframework.jdbc.core.JdbcTemplate;
import ru.pospelov.etl.engine.conversion.TypeConverter;
import ru.pospelov.etl.engine.model.EtlBatch;

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
    private final Consumer<EtlBatch> batchConsumer;
    private final TypeConverter typeConverter;
    private final String jobId;

    JdbcPartitionQueryTask(JdbcTemplate jdbcTemplate,
                           String query,
                           int part,
                           int streamBatchSize,
                           String keyColumn,
                           Consumer<EtlBatch> batchConsumer,
                           TypeConverter typeConverter,
                           String jobId) {
        this.jdbcTemplate = jdbcTemplate;
        this.query = query;
        this.part = part;
        this.streamBatchSize = streamBatchSize;
        this.keyColumn = keyColumn;
        this.batchConsumer = batchConsumer;
        this.typeConverter = typeConverter;
        this.jobId = jobId;
    }

    @Override
    public void run() {
        JdbcStreamingResultSetExtractor extractor = new JdbcStreamingResultSetExtractor(
                "sql-part-" + part,
                streamBatchSize,
                keyColumn,
                batchConsumer,
                typeConverter,
                jobId
        );
        jdbcTemplate.query(query, extractor);
    }
}
