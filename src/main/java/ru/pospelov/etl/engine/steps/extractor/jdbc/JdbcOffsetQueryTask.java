package ru.pospelov.etl.engine.steps.extractor.jdbc;

import org.springframework.jdbc.core.JdbcTemplate;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;
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
    private final Consumer<Collection<EtlRecord>> batchConsumer;

    JdbcOffsetQueryTask(JdbcTemplate jdbcTemplate,
                        String baseQuery,
                        int offset,
                        int streamBatchSize,
                        String keyColumn,
                        Consumer<Collection<EtlRecord>> batchConsumer) {
        this.jdbcTemplate = jdbcTemplate;
        this.baseQuery = baseQuery;
        this.offset = offset;
        this.streamBatchSize = streamBatchSize;
        this.keyColumn = keyColumn;
        this.batchConsumer = batchConsumer;
    }

    @Override
    public void run() {
        String pagedQuery = baseQuery +
                " OFFSET " + offset + " ROWS FETCH NEXT " + streamBatchSize + " ROWS ONLY";

        JdbcStreamingResultSetExtractor extractor = new JdbcStreamingResultSetExtractor(
                "sql",
                streamBatchSize,
                keyColumn,
                batchConsumer
        );
        jdbcTemplate.query(pagedQuery, extractor);
    }
}
