package ru.pospelov.etl.engine.steps.extractor.jdbc;

import org.springframework.jdbc.core.JdbcTemplate;
import ru.pospelov.etl.engine.conversion.TypeConverter;
import ru.pospelov.etl.engine.model.EtlBatch;

import java.util.function.Consumer;

/**
 * Задача выборки по offset/batchSize.
 */
final class JdbcOffsetQueryTask implements Runnable {

    private final JdbcTemplate jdbcTemplate;
    private final String baseQuery;
    private final int offset;
    private final int streamBatchSize;
    private final String keyColumn;
    private final Consumer<EtlBatch> batchConsumer;
    private final TypeConverter typeConverter;
    private final String jobId;

    JdbcOffsetQueryTask(JdbcTemplate jdbcTemplate,
                        String baseQuery,
                        int offset,
                        int streamBatchSize,
                        String keyColumn,
                        Consumer<EtlBatch> batchConsumer,
                        TypeConverter typeConverter,
                        String jobId) {
        this.jdbcTemplate = jdbcTemplate;
        this.baseQuery = baseQuery;
        this.offset = offset;
        this.streamBatchSize = streamBatchSize;
        this.keyColumn = keyColumn;
        this.batchConsumer = batchConsumer;
        this.typeConverter = typeConverter;
        this.jobId = jobId;
    }

    @Override
    public void run() {
        String pagedQuery = baseQuery +
                " OFFSET " + offset + " ROWS FETCH NEXT " + streamBatchSize + " ROWS ONLY";

        JdbcStreamingResultSetExtractor extractor = new JdbcStreamingResultSetExtractor(
                "sql",
                streamBatchSize,
                keyColumn,
                batchConsumer,
                typeConverter,
                jobId
        );
        jdbcTemplate.query(pagedQuery, extractor);
    }
}
