package ru.pospelov.etl.engine.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EtlBulkRecord}.
 */
class EtlBulkRecordTest {

    @Test
    void testCreation() {
        List<EtlRecord> records = createTestRecords();
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertNotNull(bulkRecord);
    }

    @Test
    void testEmptyRecordsThrowsException() {
        List<EtlRecord> emptyRecords = new ArrayList<>();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new EtlBulkRecord(emptyRecords)
        );

        assertTrue(exception.getMessage().contains("Empty record set"));
    }

    @Test
    void testGetColumnOrdinals() {
        List<EtlRecord> records = createTestRecords();
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        Set<Integer> ordinals = bulkRecord.getColumnOrdinals();

        assertNotNull(ordinals);
        assertEquals(3, ordinals.size());
        assertTrue(ordinals.contains(1));
        assertTrue(ordinals.contains(2));
        assertTrue(ordinals.contains(3));
    }

    @Test
    void testGetColumnName() {
        List<EtlRecord> records = createTestRecords();
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        // Verify all expected columns are present (order may vary)
        Set<Integer> ordinals = bulkRecord.getColumnOrdinals();
        Set<String> columnNames = new java.util.HashSet<>();
        for (Integer ordinal : ordinals) {
            columnNames.add(bulkRecord.getColumnName(ordinal));
        }

        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("name"));
        assertTrue(columnNames.contains("amount"));
    }

    @Test
    void testGetColumnType() {
        List<EtlRecord> records = createTestRecords();
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        // Find column ordinals by name (since order is not guaranteed)
        int idOrdinal = findColumnOrdinal(bulkRecord, "id");
        int nameOrdinal = findColumnOrdinal(bulkRecord, "name");
        int amountOrdinal = findColumnOrdinal(bulkRecord, "amount");

        assertEquals(Types.INTEGER, bulkRecord.getColumnType(idOrdinal));
        assertEquals(Types.VARCHAR, bulkRecord.getColumnType(nameOrdinal));
        assertEquals(Types.DECIMAL, bulkRecord.getColumnType(amountOrdinal));
    }

    @Test
    void testIterationAndGetRowData() throws Exception {
        List<EtlRecord> records = createTestRecords();
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        // Find column ordinals by name
        int idOrdinal = findColumnOrdinal(bulkRecord, "id");
        int nameOrdinal = findColumnOrdinal(bulkRecord, "name");
        int amountOrdinal = findColumnOrdinal(bulkRecord, "amount");

        // First record
        assertTrue(bulkRecord.next());
        Object[] row1 = bulkRecord.getRowData();
        assertEquals(3, row1.length);
        assertEquals(1, row1[idOrdinal - 1]);
        assertEquals("Alice", row1[nameOrdinal - 1]);
        assertEquals(new BigDecimal("100.50"), row1[amountOrdinal - 1]);

        // Second record
        assertTrue(bulkRecord.next());
        Object[] row2 = bulkRecord.getRowData();
        assertEquals(2, row2[idOrdinal - 1]);
        assertEquals("Bob", row2[nameOrdinal - 1]);
        assertEquals(new BigDecimal("200.75"), row2[amountOrdinal - 1]);

        // No more records
        assertFalse(bulkRecord.next());
    }

    // ============ Type mapping tests ============

    @Test
    void testMapJavaToSqlType_LocalDate() {
        LocalDate date = LocalDate.of(2024, 1, 15);
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_date", date);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.DATE, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_Instant() {
        Instant instant = Instant.now();
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_instant", instant);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.TIMESTAMP, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_LocalTime() {
        LocalTime time = LocalTime.of(14, 30, 45);
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_time", time);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.TIME, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_OffsetDateTime() {
        OffsetDateTime odt = OffsetDateTime.now();
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_offset", odt);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_SqlTimestamp() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_timestamp", timestamp);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.TIMESTAMP, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_SqlDate() {
        Date date = Date.valueOf("2024-01-15");
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_date", date);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.DATE, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_SqlTime() {
        Time time = Time.valueOf("14:30:45");
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_time", time);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.TIME, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_Integer() {
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_int", 123);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.INTEGER, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_Long() {
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_long", 123456789L);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.BIGINT, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_BigDecimal() {
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_decimal", new BigDecimal("1234.56"));

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.DECIMAL, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_Float() {
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_float", 1.23f);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.FLOAT, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_Double() {
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_double", 1.23456789);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.DOUBLE, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_Boolean() {
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_bool", true);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.BOOLEAN, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_String() {
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_string", "test value");

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.VARCHAR, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_ByteArray() {
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_binary", new byte[]{1, 2, 3, 4});

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.VARBINARY, bulkRecord.getColumnType(1));
    }

    @Test
    void testMapJavaToSqlType_NullValue() {
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("test_null", null);

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.VARCHAR, bulkRecord.getColumnType(1));
    }

    @Test
    void testMixedTypes() {
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 1L);
        record.put("id", 1);
        record.put("name", "test");
        record.put("created_at", LocalDate.of(2024, 1, 15));
        record.put("updated_at", Instant.now());
        record.put("amount", new BigDecimal("123.45"));
        record.put("is_active", true);
        record.put("data", new byte[]{1, 2, 3});

        List<EtlRecord> records = List.of(record);
        EtlBulkRecord bulkRecord = new EtlBulkRecord(records);

        assertEquals(Types.INTEGER, bulkRecord.getColumnType(bulkRecord.getColumnOrdinals().stream()
                .filter(i -> "id".equals(bulkRecord.getColumnName(i)))
                .findFirst().orElse(0)));

        assertEquals(Types.VARCHAR, bulkRecord.getColumnType(bulkRecord.getColumnOrdinals().stream()
                .filter(i -> "name".equals(bulkRecord.getColumnName(i)))
                .findFirst().orElse(0)));

        assertEquals(Types.DATE, bulkRecord.getColumnType(bulkRecord.getColumnOrdinals().stream()
                .filter(i -> "created_at".equals(bulkRecord.getColumnName(i)))
                .findFirst().orElse(0)));

        assertEquals(Types.TIMESTAMP, bulkRecord.getColumnType(bulkRecord.getColumnOrdinals().stream()
                .filter(i -> "updated_at".equals(bulkRecord.getColumnName(i)))
                .findFirst().orElse(0)));

        assertEquals(Types.DECIMAL, bulkRecord.getColumnType(bulkRecord.getColumnOrdinals().stream()
                .filter(i -> "amount".equals(bulkRecord.getColumnName(i)))
                .findFirst().orElse(0)));

        assertEquals(Types.BOOLEAN, bulkRecord.getColumnType(bulkRecord.getColumnOrdinals().stream()
                .filter(i -> "is_active".equals(bulkRecord.getColumnName(i)))
                .findFirst().orElse(0)));

        assertEquals(Types.VARBINARY, bulkRecord.getColumnType(bulkRecord.getColumnOrdinals().stream()
                .filter(i -> "data".equals(bulkRecord.getColumnName(i)))
                .findFirst().orElse(0)));
    }

    // ============ Helper methods ============

    private List<EtlRecord> createTestRecords() {
        List<EtlRecord> records = new ArrayList<>();

        EtlRecord record1 = new EtlRecord(Instant.now(), "partition-0", 1L);
        record1.put("id", 1);
        record1.put("name", "Alice");
        record1.put("amount", new BigDecimal("100.50"));
        records.add(record1);

        EtlRecord record2 = new EtlRecord(Instant.now(), "partition-0", 2L);
        record2.put("id", 2);
        record2.put("name", "Bob");
        record2.put("amount", new BigDecimal("200.75"));
        records.add(record2);

        return records;
    }

    private int findColumnOrdinal(EtlBulkRecord bulkRecord, String columnName) {
        for (Integer ordinal : bulkRecord.getColumnOrdinals()) {
            if (columnName.equals(bulkRecord.getColumnName(ordinal))) {
                return ordinal;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }
}
