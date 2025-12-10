package ru.pospelov.etl.engine.steps.extractor.jdbc;

import org.springframework.jdbc.core.ResultSetExtractor;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Streaming ResultSetExtractor that processes records in batches.
 * Выделен отдельно, чтобы связь с JdbcExtractor была явной.
 */
final class JdbcStreamingResultSetExtractor implements ResultSetExtractor<Void> {

    private final String sourcePartition;
    private final int batchSize;
    private final String keyColumn;
    private final Consumer<Collection<EtlRecord>> batchConsumer;

    JdbcStreamingResultSetExtractor(String sourcePartition,
                                    int batchSize,
                                    String keyColumn,
                                    Consumer<Collection<EtlRecord>> batchConsumer) {
        this.sourcePartition = sourcePartition;
        this.batchSize = batchSize;
        this.keyColumn = keyColumn;
        this.batchConsumer = batchConsumer;
    }

    @Override
    public Void extractData(ResultSet rs) throws SQLException {
        rs.setFetchSize(10000);

        ResultSetMetaData md = rs.getMetaData();
        List<EtlRecord> currentBatch = new ArrayList<>(batchSize);

        while (rs.next()) {
            EtlRecord record = new EtlRecord(Instant.now(), sourcePartition, rs.getRow());
            for (int i = 1; i <= md.getColumnCount(); i++) {
                String col = md.getColumnLabel(i);
                Object val = rs.getObject(i);
                record.put(col, val);
            }
            if (!keyColumn.isEmpty()) {
                record.put("key", rs.getObject(keyColumn));
            }
            currentBatch.add(record);

            if (currentBatch.size() >= batchSize) {
                batchConsumer.accept(new ArrayList<>(currentBatch));
                currentBatch.clear();
            }
        }

        if (!currentBatch.isEmpty()) {
            batchConsumer.accept(currentBatch);
        }

        return null;
    }
}
