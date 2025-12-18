package ru.pospelov.etl.engine.model;

import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EtlBatch}.
 */
class EtlBatchTest {

    @Test
    void testCreationWithMetadata() {
        // Create sample records
        List<EtlRecord> records = new ArrayList<>();
        EtlRecord record1 = new EtlRecord(Instant.now(), "partition-0", 1L);
        record1.put("id", 1);
        record1.put("name", "test");
        records.add(record1);

        // Create metadata
        Map<String, ColumnMetadata> metadata = new LinkedHashMap<>();
        metadata.put("id", new ColumnMetadata("id", Types.INTEGER, "INT", 0, 0, false));
        metadata.put("name", new ColumnMetadata("name", Types.VARCHAR, "NVARCHAR", 100, 0, true));

        // Create batch
        EtlBatch batch = new EtlBatch(records, metadata);

        assertNotNull(batch);
        assertEquals(1, batch.size());
        assertFalse(batch.isEmpty());
        assertTrue(batch.hasMetadata());
        assertNotNull(batch.getColumnMetadata());
        assertEquals(2, batch.getColumnMetadata().size());
        assertTrue(batch.getColumnMetadata().containsKey("id"));
        assertTrue(batch.getColumnMetadata().containsKey("name"));
    }

    @Test
    void testCreationWithoutMetadata() {
        // Create sample records
        List<EtlRecord> records = new ArrayList<>();
        EtlRecord record1 = new EtlRecord(Instant.now(), "kafka-topic-0", 100L);
        record1.put("key", "test-key");
        record1.put("value", "test-value");
        records.add(record1);

        // Create batch without metadata (Kafka source)
        EtlBatch batch = new EtlBatch(records, null);

        assertNotNull(batch);
        assertEquals(1, batch.size());
        assertFalse(batch.isEmpty());
        assertFalse(batch.hasMetadata());
        assertNull(batch.getColumnMetadata());
    }

    @Test
    void testEmptyBatch() {
        EtlBatch batch = new EtlBatch(Collections.emptyList(), null);

        assertTrue(batch.isEmpty());
        assertEquals(0, batch.size());
        assertFalse(batch.hasMetadata());
    }

    @Test
    void testUnmodifiableMetadata() {
        Map<String, ColumnMetadata> metadata = new HashMap<>();
        metadata.put("id", new ColumnMetadata("id", Types.INTEGER, "INT", 0, 0, false));

        List<EtlRecord> records = List.of(
                new EtlRecord(Instant.now(), "partition-0", 1L)
        );

        EtlBatch batch = new EtlBatch(records, metadata);

        // Get metadata should return unmodifiable map
        Map<String, ColumnMetadata> returnedMetadata = batch.getColumnMetadata();
        assertNotNull(returnedMetadata);

        // Attempting to modify should throw exception
        assertThrows(UnsupportedOperationException.class, () -> {
            returnedMetadata.put("new_col", new ColumnMetadata("new_col", Types.VARCHAR, "VARCHAR", 10, 0, true));
        });
    }

    @Test
    void testMultipleRecords() {
        List<EtlRecord> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            EtlRecord record = new EtlRecord(Instant.now(), "partition-0", (long) i);
            record.put("id", i);
            records.add(record);
        }

        EtlBatch batch = new EtlBatch(records, null);

        assertEquals(100, batch.size());
        assertFalse(batch.isEmpty());
        assertEquals(100, batch.getRecords().size());
    }

    @Test
    void testToString() {
        List<EtlRecord> records = new ArrayList<>();
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("id", 1);
        records.add(record);

        EtlBatch batch = new EtlBatch(records, null);

        String str = batch.toString();
        assertNotNull(str);
        // toString should not contain records (excluded in @ToString)
        assertFalse(str.contains("EtlRecord"));
    }
}
