package ru.pospelov.etl.engine.conversion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.pospelov.etl.engine.exception.TypeConversionException;
import ru.pospelov.etl.engine.model.ColumnMetadata;
import ru.pospelov.etl.engine.model.SqlVariantValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.*;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TypeConverter}.
 */
class TypeConverterTest {

    private TypeConverter converter;
    private ObjectMapper objectMapper;
    private static final String TEST_JOB_ID = "test-job";

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        converter = new TypeConverter(objectMapper);
    }

    // ============ convertToAvro tests ============

    @Test
    void testConvertToAvro_LocalDate() {
        Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        Schema.Field field = new Schema.Field("test_date", dateSchema);

        LocalDate date = LocalDate.of(2024, 1, 15);
        Object result = converter.convertToAvro(date, field, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof Integer);
        assertEquals((int) date.toEpochDay(), result);
    }

    @Test
    void testConvertToAvro_Instant() {
        Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema.Field field = new Schema.Field("test_timestamp", timestampSchema);

        Instant instant = Instant.parse("2024-01-15T10:30:45.123Z");
        Object result = converter.convertToAvro(instant, field, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof Long);
        assertEquals(instant.toEpochMilli(), result);
    }

    @Test
    void testConvertToAvro_LocalTime() {
        Schema timeSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema.Field field = new Schema.Field("test_time", timeSchema);

        LocalTime time = LocalTime.of(14, 30, 45, 123456000);
        Object result = converter.convertToAvro(time, field, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof Long);
        assertEquals(time.toNanoOfDay() / 1000, result);
    }

    @Test
    void testConvertToAvro_BigDecimal() {
        Schema decimalSchema = LogicalTypes.decimal(18, 2).addToSchema(Schema.create(Schema.Type.BYTES));
        Schema.Field field = new Schema.Field("test_decimal", decimalSchema);

        BigDecimal decimal = new BigDecimal("1234.56");
        Object result = converter.convertToAvro(decimal, field, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof ByteBuffer);
    }

    @Test
    void testConvertToAvro_ByteArray() {
        Schema bytesSchema = Schema.create(Schema.Type.BYTES);
        Schema.Field field = new Schema.Field("test_bytes", bytesSchema);

        byte[] bytes = new byte[]{1, 2, 3, 4};
        Object result = converter.convertToAvro(bytes, field, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof ByteBuffer);

        ByteBuffer buffer = (ByteBuffer) result;
        byte[] resultBytes = new byte[buffer.remaining()];
        buffer.get(resultBytes);
        assertArrayEquals(bytes, resultBytes);
    }

    @Test
    void testConvertToAvro_SqlVariantValue() {
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema.Field field = new Schema.Field("test_variant", stringSchema);

        SqlVariantValue variant = new SqlVariantValue("decimal(18,2)", "1234.56", "plain");
        Object result = converter.convertToAvro(variant, field, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof String);

        String json = (String) result;
        assertTrue(json.contains("\"v\":1"));
        assertTrue(json.contains("\"t\":\"decimal(18,2)\""));
        assertTrue(json.contains("\"val\":\"1234.56\""));
    }

    @Test
    void testConvertToAvro_OffsetDateTime() {
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema.Field field = new Schema.Field("test_offset", stringSchema);

        OffsetDateTime odt = OffsetDateTime.parse("2024-01-15T10:30:45.123+03:00");
        Object result = converter.convertToAvro(odt, field, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof String);
        assertTrue(((String) result).contains("+03:00"));
    }

    @Test
    void testConvertToAvro_SimpleTypes() {
        // String
        Schema.Field stringField = new Schema.Field("test_string", Schema.create(Schema.Type.STRING));
        assertEquals("test", converter.convertToAvro("test", stringField, TEST_JOB_ID));

        // Integer
        Schema.Field intField = new Schema.Field("test_int", Schema.create(Schema.Type.INT));
        assertEquals(123, converter.convertToAvro(123, intField, TEST_JOB_ID));

        // Long
        Schema.Field longField = new Schema.Field("test_long", Schema.create(Schema.Type.LONG));
        assertEquals(123L, converter.convertToAvro(123L, longField, TEST_JOB_ID));

        // Boolean
        Schema.Field boolField = new Schema.Field("test_bool", Schema.create(Schema.Type.BOOLEAN));
        assertEquals(true, converter.convertToAvro(true, boolField, TEST_JOB_ID));

        // Float
        Schema.Field floatField = new Schema.Field("test_float", Schema.create(Schema.Type.FLOAT));
        assertEquals(1.23f, converter.convertToAvro(1.23f, floatField, TEST_JOB_ID));

        // Double
        Schema.Field doubleField = new Schema.Field("test_double", Schema.create(Schema.Type.DOUBLE));
        assertEquals(1.23, converter.convertToAvro(1.23, doubleField, TEST_JOB_ID));
    }

    @Test
    void testConvertToAvro_NullValue() {
        Schema nullableSchema = SchemaBuilder.unionOf()
                .nullType()
                .and()
                .stringType()
                .endUnion();
        Schema.Field field = new Schema.Field("test_nullable", nullableSchema);

        Object result = converter.convertToAvro(null, field, TEST_JOB_ID);
        assertNull(result);
    }

    @Test
    void testConvertToAvro_TypeMismatch() {
        Schema intSchema = Schema.create(Schema.Type.INT);
        Schema.Field field = new Schema.Field("test_int", intSchema);

        // Пытаемся передать String в int поле
        TypeConversionException exception = assertThrows(TypeConversionException.class, () ->
                converter.convertToAvro("not an int", field, TEST_JOB_ID)
        );

        assertTrue(exception.getMessage().contains("Cannot convert"));
        assertTrue(exception.getMessage().contains("test_int"));
    }

    @Test
    void testConvertToAvro_LocalDateWrongType() {
        // LocalDate требует int, но передаем long schema
        Schema longSchema = Schema.create(Schema.Type.LONG);
        Schema.Field field = new Schema.Field("test_date", longSchema);

        LocalDate date = LocalDate.of(2024, 1, 15);

        TypeConversionException exception = assertThrows(TypeConversionException.class, () ->
                converter.convertToAvro(date, field, TEST_JOB_ID)
        );

        assertTrue(exception.getMessage().contains("LocalDate"));
        assertTrue(exception.getMessage().contains("must be mapped to Avro int"));
    }

    // ============ convertToJdbc tests ============

    @Test
    void testConvertToJdbc_LocalDate() {
        LocalDate date = LocalDate.of(2024, 1, 15);
        Object result = converter.convertToJdbc(date, null, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof Date);
        assertEquals(Date.valueOf(date), result);
    }

    @Test
    void testConvertToJdbc_Instant() {
        Instant instant = Instant.parse("2024-01-15T10:30:45.123Z");
        Object result = converter.convertToJdbc(instant, null, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof Timestamp);
        assertEquals(Timestamp.from(instant), result);
    }

    @Test
    void testConvertToJdbc_LocalTime() {
        LocalTime time = LocalTime.of(14, 30, 45);
        Object result = converter.convertToJdbc(time, null, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof Time);
        assertEquals(Time.valueOf(time), result);
    }

    @Test
    void testConvertToJdbc_OffsetDateTime() {
        OffsetDateTime odt = OffsetDateTime.parse("2024-01-15T10:30:45.123+03:00");
        Object result = converter.convertToJdbc(odt, null, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof String);
        assertTrue(((String) result).contains("+03:00"));
    }

    @Test
    void testConvertToJdbc_ByteBuffer() {
        byte[] bytes = new byte[]{1, 2, 3, 4};
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        Object result = converter.convertToJdbc(buffer, null, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof byte[]);
        assertArrayEquals(bytes, (byte[]) result);
    }

    @Test
    void testConvertToJdbc_SqlVariantValue() {
        SqlVariantValue variant = new SqlVariantValue("int", "123", "plain");
        Object result = converter.convertToJdbc(variant, null, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof Integer);
        assertEquals(123, result);
    }

    @Test
    void testConvertToJdbc_SqlVariantJson() {
        String json = "{\"v\":1,\"t\":\"decimal(18,2)\",\"val\":\"1234.56\",\"enc\":\"plain\"}";

        ColumnMetadata metadata = new ColumnMetadata("test_col", Types.OTHER, "sql_variant", 0, 0, true);
        Object result = converter.convertToJdbc(json, metadata, TEST_JOB_ID);

        assertNotNull(result);
        // Now returns SqlVariantValue instead of unpacked value (for bulk copy compatibility)
        assertTrue(result instanceof SqlVariantValue);

        // Verify the SqlVariantValue contains correct data
        SqlVariantValue variant = (SqlVariantValue) result;
        assertEquals("decimal(18,2)", variant.getSqlType());
        assertEquals("1234.56", variant.getValue());
        assertEquals("plain", variant.getEncoding());

        // Verify unpacking works correctly
        Object unpacked = converter.unpackSqlVariant(variant, TEST_JOB_ID);
        assertTrue(unpacked instanceof BigDecimal);
        assertEquals(new BigDecimal("1234.56"), unpacked);
    }

    @Test
    void testConvertToJdbc_SimpleTypes() {
        // String
        assertEquals("test", converter.convertToJdbc("test", null, TEST_JOB_ID));

        // Integer
        assertEquals(123, converter.convertToJdbc(123, null, TEST_JOB_ID));

        // Long
        assertEquals(123L, converter.convertToJdbc(123L, null, TEST_JOB_ID));

        // Boolean
        assertEquals(true, converter.convertToJdbc(true, null, TEST_JOB_ID));

        // Float
        assertEquals(1.23f, converter.convertToJdbc(1.23f, null, TEST_JOB_ID));

        // Double
        assertEquals(1.23, converter.convertToJdbc(1.23, null, TEST_JOB_ID));

        // BigDecimal
        BigDecimal bd = new BigDecimal("123.45");
        assertEquals(bd, converter.convertToJdbc(bd, null, TEST_JOB_ID));

        // byte[]
        byte[] bytes = new byte[]{1, 2, 3};
        assertArrayEquals(bytes, (byte[]) converter.convertToJdbc(bytes, null, TEST_JOB_ID));
    }

    @Test
    void testConvertToJdbc_NullValue() {
        Object result = converter.convertToJdbc(null, null, TEST_JOB_ID);
        assertNull(result);
    }

    // ============ Avro logical types to JDBC tests ============

    @Test
    void testAvroDateToLocalDate() {
        TypeConverter converter = new TypeConverter();
        Integer avroDate = (int) LocalDate.of(2024, 3, 15).toEpochDay();
        ColumnMetadata meta = new ColumnMetadata("order_date", Types.DATE, "DATE", 0, 0, true);

        Object result = converter.convertToJdbc(avroDate, meta, TEST_JOB_ID);

        assertEquals(LocalDate.class, result.getClass());
        assertEquals(LocalDate.of(2024, 3, 15), result);
    }

    @Test
    void testAvroTimestampToInstant() {
        TypeConverter converter = new TypeConverter();
        Instant expected = Instant.parse("2024-03-15T10:30:45.123Z");
        Long avroTimestamp = expected.toEpochMilli();
        ColumnMetadata meta = new ColumnMetadata("created_at", Types.TIMESTAMP, "DATETIME2", 7, 0, true);

        Object result = converter.convertToJdbc(avroTimestamp, meta, TEST_JOB_ID);

        // convertToJdbc returns java.sql.Timestamp for JDBC/BulkCopy compatibility
        assertEquals(Timestamp.class, result.getClass());
        assertEquals(expected, ((Timestamp) result).toInstant());
    }

    @Test
    void testAvroDecimalToBigDecimal() {
        TypeConverter converter = new TypeConverter();
        BigDecimal expected = new BigDecimal("12345.67");

        // Encode as Avro decimal
        BigInteger unscaled = expected.unscaledValue();
        ByteBuffer avroDecimal = ByteBuffer.wrap(unscaled.toByteArray());

        ColumnMetadata meta = new ColumnMetadata("amount", Types.DECIMAL, "DECIMAL", 18, 2, true);

        Object result = converter.convertToJdbc(avroDecimal, meta, TEST_JOB_ID);

        assertEquals(BigDecimal.class, result.getClass());
        assertEquals(expected, result);
    }

    // ============ createSqlVariant tests ============

    @Test
    void testCreateSqlVariant_Integer() {
        SqlVariantValue variant = converter.createSqlVariant(123, "int", null, null, null, TEST_JOB_ID);

        assertNotNull(variant);
        assertEquals("int", variant.getSqlType());
        assertEquals("123", variant.getValue());
        assertEquals("plain", variant.getEncoding());
    }

    @Test
    void testCreateSqlVariant_Decimal() {
        BigDecimal decimal = new BigDecimal("1234.56");
        SqlVariantValue variant = converter.createSqlVariant(decimal, "decimal", 18, 2, null, TEST_JOB_ID);

        assertNotNull(variant);
        assertEquals("decimal(18,2)", variant.getSqlType());
        assertEquals("1234.56", variant.getValue());
        assertEquals("plain", variant.getEncoding());
    }

    @Test
    void testCreateSqlVariant_Varchar() {
        SqlVariantValue variant = converter.createSqlVariant("test", "varchar", null, null, 50, TEST_JOB_ID);

        assertNotNull(variant);
        assertEquals("varchar(50)", variant.getSqlType());
        assertEquals("test", variant.getValue());
        assertEquals("plain", variant.getEncoding());
    }

    @Test
    void testCreateSqlVariant_VarcharMax() {
        SqlVariantValue variant = converter.createSqlVariant("test", "varchar", null, null, -1, TEST_JOB_ID);

        assertNotNull(variant);
        assertEquals("varchar(max)", variant.getSqlType());
        assertEquals("test", variant.getValue());
        assertEquals("plain", variant.getEncoding());
    }

    @Test
    void testCreateSqlVariant_Datetime2() {
        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45.123");
        SqlVariantValue variant = converter.createSqlVariant(timestamp, "datetime2", null, 7, null, TEST_JOB_ID);

        assertNotNull(variant);
        assertEquals("datetime2(7)", variant.getSqlType());
        assertTrue(variant.getValue().contains("2024-01-15"));
        assertEquals("plain", variant.getEncoding());
    }

    @Test
    void testCreateSqlVariant_Varbinary() {
        byte[] bytes = new byte[]{1, 2, 3, 4};
        SqlVariantValue variant = converter.createSqlVariant(bytes, "varbinary", null, null, 100, TEST_JOB_ID);

        assertNotNull(variant);
        assertEquals("varbinary(100)", variant.getSqlType());
        assertEquals("base64", variant.getEncoding());

        // Verify can decode back
        byte[] decoded = Base64.getDecoder().decode(variant.getValue());
        assertArrayEquals(bytes, decoded);
    }

    @Test
    void testCreateSqlVariant_NullValue() {
        SqlVariantValue variant = converter.createSqlVariant(null, "int", null, null, null, TEST_JOB_ID);

        assertNotNull(variant);
        assertEquals("int", variant.getSqlType());
        assertNull(variant.getValue());
        assertEquals("plain", variant.getEncoding());
    }

    @Test
    void testCreateSqlVariant_NullBaseType() {
        TypeConversionException exception = assertThrows(TypeConversionException.class, () ->
                converter.createSqlVariant(123, null, null, null, null, TEST_JOB_ID)
        );

        assertTrue(exception.getMessage().contains("SQL_VARIANT_PROPERTY"));
        assertTrue(exception.getMessage().contains("BaseType"));
    }

    // ============ unpackSqlVariant tests ============

    @Test
    void testUnpackSqlVariant_Int() {
        SqlVariantValue variant = new SqlVariantValue("int", "123", "plain");
        Object result = converter.unpackSqlVariant(variant, TEST_JOB_ID);

        assertTrue(result instanceof Integer);
        assertEquals(123, result);
    }

    @Test
    void testUnpackSqlVariant_Bigint() {
        SqlVariantValue variant = new SqlVariantValue("bigint", "9223372036854775807", "plain");
        Object result = converter.unpackSqlVariant(variant, TEST_JOB_ID);

        assertTrue(result instanceof Long);
        assertEquals(9223372036854775807L, result);
    }

    @Test
    void testUnpackSqlVariant_Bit() {
        SqlVariantValue variant1 = new SqlVariantValue("bit", "true", "plain");
        SqlVariantValue variant2 = new SqlVariantValue("bit", "1", "plain");
        SqlVariantValue variant3 = new SqlVariantValue("bit", "false", "plain");
        SqlVariantValue variant4 = new SqlVariantValue("bit", "0", "plain");

        assertEquals(true, converter.unpackSqlVariant(variant1, TEST_JOB_ID));
        assertEquals(true, converter.unpackSqlVariant(variant2, TEST_JOB_ID));
        assertEquals(false, converter.unpackSqlVariant(variant3, TEST_JOB_ID));
        assertEquals(false, converter.unpackSqlVariant(variant4, TEST_JOB_ID));
    }

    @Test
    void testUnpackSqlVariant_Decimal() {
        SqlVariantValue variant = new SqlVariantValue("decimal(18,2)", "1234.56", "plain");
        Object result = converter.unpackSqlVariant(variant, TEST_JOB_ID);

        assertTrue(result instanceof BigDecimal);
        assertEquals(new BigDecimal("1234.56"), result);
    }

    @Test
    void testUnpackSqlVariant_Varchar() {
        SqlVariantValue variant = new SqlVariantValue("varchar(50)", "test value", "plain");
        Object result = converter.unpackSqlVariant(variant, TEST_JOB_ID);

        assertTrue(result instanceof String);
        assertEquals("test value", result);
    }

    @Test
    void testUnpackSqlVariant_Varbinary() {
        byte[] bytes = new byte[]{1, 2, 3, 4};
        String base64 = Base64.getEncoder().encodeToString(bytes);
        SqlVariantValue variant = new SqlVariantValue("varbinary(100)", base64, "base64");

        Object result = converter.unpackSqlVariant(variant, TEST_JOB_ID);

        assertTrue(result instanceof byte[]);
        assertArrayEquals(bytes, (byte[]) result);
    }

    @Test
    void testUnpackSqlVariant_Date() {
        SqlVariantValue variant = new SqlVariantValue("date", "2024-01-15", "plain");
        Object result = converter.unpackSqlVariant(variant, TEST_JOB_ID);

        assertTrue(result instanceof String);
        assertEquals("2024-01-15", result);
    }

    @Test
    void testUnpackSqlVariant_Datetime2() {
        // Use SQL Server format (yyyy-MM-dd HH:mm:ss.nnnnnnn) instead of ISO-8601
        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45.123");
        SqlVariantValue variant = new SqlVariantValue("datetime2(7)", timestamp.toString(), "plain");

        Object result = converter.unpackSqlVariant(variant, TEST_JOB_ID);

        assertTrue(result instanceof String);
        assertEquals(timestamp.toString(), result);
    }

    @Test
    void testUnpackSqlVariant_Time() {
        SqlVariantValue variant = new SqlVariantValue("time(7)", "14:30:45", "plain");
        Object result = converter.unpackSqlVariant(variant, TEST_JOB_ID);

        assertTrue(result instanceof String);
        assertEquals("14:30:45", result);
    }

    @Test
    void testUnpackSqlVariant_Real() {
        SqlVariantValue variant = new SqlVariantValue("real", "1.23", "plain");
        Object result = converter.unpackSqlVariant(variant, TEST_JOB_ID);

        assertTrue(result instanceof Float);
        assertEquals(1.23f, (Float) result, 0.001f);
    }

    @Test
    void testUnpackSqlVariant_Float() {
        SqlVariantValue variant = new SqlVariantValue("float", "1.234567", "plain");
        Object result = converter.unpackSqlVariant(variant, TEST_JOB_ID);

        assertTrue(result instanceof Double);
        assertEquals(1.234567, (Double) result, 0.000001);
    }

    @Test
    void testUnpackSqlVariant_NullValue() {
        SqlVariantValue variant = new SqlVariantValue("int", null, "plain");
        Object result = converter.unpackSqlVariant(variant, TEST_JOB_ID);

        assertNull(result);
    }

    @Test
    void testUnpackSqlVariant_UnsupportedType() {
        SqlVariantValue variant = new SqlVariantValue("unsupported_type", "value", "plain");

        TypeConversionException exception = assertThrows(TypeConversionException.class, () ->
                converter.unpackSqlVariant(variant, TEST_JOB_ID)
        );

        assertTrue(exception.getMessage().contains("Unsupported sql_variant base type"));
    }

    @Test
    void testUnpackSqlVariant_InvalidValue() {
        SqlVariantValue variant = new SqlVariantValue("int", "not a number", "plain");

        TypeConversionException exception = assertThrows(TypeConversionException.class, () ->
                converter.unpackSqlVariant(variant, TEST_JOB_ID)
        );

        assertTrue(exception.getMessage().contains("Failed to parse"));
    }

    // ============ sqlVariantToJson / sqlVariantFromJson tests ============

    @Test
    void testSqlVariantJsonRoundtrip() throws Exception {
        SqlVariantValue original = new SqlVariantValue("decimal(18,2)", "1234.56", "plain");

        String json = converter.sqlVariantToJson(original);
        assertNotNull(json);

        // Verify JSON structure
        JsonNode node = objectMapper.readTree(json);
        assertEquals(1, node.get("v").asInt());
        assertEquals("decimal(18,2)", node.get("t").asText());
        assertEquals("1234.56", node.get("val").asText());
        assertEquals("plain", node.get("enc").asText());

        // Round trip
        SqlVariantValue restored = converter.sqlVariantFromJson(json, TEST_JOB_ID);
        assertEquals(original, restored);
    }

    @Test
    void testSqlVariantJsonRoundtrip_Binary() throws Exception {
        byte[] bytes = new byte[]{1, 2, 3, 4};
        String base64 = Base64.getEncoder().encodeToString(bytes);
        SqlVariantValue original = new SqlVariantValue("varbinary(100)", base64, "base64");

        String json = converter.sqlVariantToJson(original);
        SqlVariantValue restored = converter.sqlVariantFromJson(json, TEST_JOB_ID);

        assertEquals(original, restored);
    }

    @Test
    void testSqlVariantJsonRoundtrip_NullValue() throws Exception {
        SqlVariantValue original = new SqlVariantValue("int", null, "plain");

        String json = converter.sqlVariantToJson(original);
        SqlVariantValue restored = converter.sqlVariantFromJson(json, TEST_JOB_ID);

        assertEquals("int", restored.getSqlType());
        assertNull(restored.getValue());
        assertEquals("plain", restored.getEncoding());
    }

    @Test
    void testSqlVariantFromJson_InvalidJson() {
        String invalidJson = "not json";

        TypeConversionException exception = assertThrows(TypeConversionException.class, () ->
                converter.sqlVariantFromJson(invalidJson, TEST_JOB_ID)
        );

        assertTrue(exception.getMessage().contains("Failed to deserialize"));
    }

    @Test
    void testSqlVariantFromJson_UnsupportedVersion() {
        String json = "{\"v\":2,\"t\":\"int\",\"val\":\"123\",\"enc\":\"plain\"}";

        TypeConversionException exception = assertThrows(TypeConversionException.class, () ->
                converter.sqlVariantFromJson(json, TEST_JOB_ID)
        );

        assertTrue(exception.getMessage().contains("Unsupported SqlVariantValue JSON version"));
        assertTrue(exception.getMessage().contains("expected 1"));
    }

    @Test
    void testSqlVariantFromJson_MissingVersion() {
        String json = "{\"t\":\"int\",\"val\":\"123\",\"enc\":\"plain\"}";

        TypeConversionException exception = assertThrows(TypeConversionException.class, () ->
                converter.sqlVariantFromJson(json, TEST_JOB_ID)
        );

        assertTrue(exception.getMessage().contains("Unsupported SqlVariantValue JSON version"));
    }

    // ============ Edge cases and integration tests ============

    @Test
    void testFullPipeline_SqlToKafkaToSql_Date() {
        LocalDate originalDate = LocalDate.of(2024, 3, 15);

        // Step 1: JDBC Extractor produces canonical LocalDate (via normalizeJdbcValue)
        // (this is already a LocalDate, so no conversion needed)

        // Step 2: Kafka Loader - Convert to Avro
        Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        Schema.Field dateField = new Schema.Field("order_date", dateSchema);

        Object avroValue = converter.convertToAvro(originalDate, dateField, TEST_JOB_ID);
        assertEquals(Integer.class, avroValue.getClass());
        assertEquals((int) originalDate.toEpochDay(), avroValue);

        // Step 3: Kafka Extractor reads int from Avro
        Integer avroDate = (Integer) avroValue;

        // Step 4: SQL Loader - Convert back to canonical type
        ColumnMetadata dateMetadata = new ColumnMetadata("order_date", Types.DATE, "DATE", 0, 0, true);
        Object sqlValue = converter.convertToJdbc(avroDate, dateMetadata, TEST_JOB_ID);

        // Should be LocalDate (canonical type)
        assertEquals(LocalDate.class, sqlValue.getClass());
        assertEquals(originalDate, sqlValue);
    }

    @Test
    void testFullPipeline_SqlToKafkaToSql_SqlVariant() throws Exception {
        // 1. JDBC Extractor: создание SqlVariantValue из sql_variant колонки
        BigDecimal originalValue = new BigDecimal("1234.56");
        SqlVariantValue variant = converter.createSqlVariant(originalValue, "decimal", 18, 2, null, TEST_JOB_ID);

        // 2. Kafka Loader: конвертация в Avro (JSON string)
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema.Field field = new Schema.Field("metadata", stringSchema);

        Object avroValue = converter.convertToAvro(variant, field, TEST_JOB_ID);
        assertTrue(avroValue instanceof String);

        // 3. Kafka Extractor: десериализация JSON обратно в SqlVariantValue
        SqlVariantValue restored = converter.sqlVariantFromJson((String) avroValue, TEST_JOB_ID);
        assertEquals(variant, restored);

        // 4. SQL Loader: распаковка в базовый тип
        Object jdbcValue = converter.unpackSqlVariant(restored, TEST_JOB_ID);
        assertTrue(jdbcValue instanceof BigDecimal);
        assertEquals(originalValue, jdbcValue);
    }

    @Test
    void testSqlVariantWithAllSupportedTypes() {
        // Проверка всех поддерживаемых sql_variant типов
        assertDoesNotThrow(() -> {
            converter.unpackSqlVariant(new SqlVariantValue("int", "123", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("smallint", "123", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("tinyint", "123", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("bigint", "123", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("bit", "1", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("real", "1.23", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("float", "1.23", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("decimal(18,2)", "123.45", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("numeric(10,4)", "123.4567", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("money", "123.45", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("varchar(50)", "test", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("nvarchar(100)", "test", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("date", "2024-01-15", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("time(7)", "14:30:45", "plain"), TEST_JOB_ID);
            // Use SQL Server format (yyyy-MM-dd HH:mm:ss.nnnnnnn) instead of ISO-8601
            converter.unpackSqlVariant(new SqlVariantValue("datetime2(7)", "2024-01-15 10:30:45.123", "plain"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("varbinary(100)", Base64.getEncoder().encodeToString(new byte[]{1, 2, 3}), "base64"), TEST_JOB_ID);
            converter.unpackSqlVariant(new SqlVariantValue("uniqueidentifier", "550e8400-e29b-41d4-a716-446655440000", "plain"), TEST_JOB_ID);
        });
    }

    // ============ Timestamp-micros support tests ============

    @Test
    void testConvertToAvro_Instant_TimestampMicros() {
        Schema timestampSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema.Field field = new Schema.Field("test_timestamp_micros", timestampSchema);

        Instant instant = Instant.parse("2024-01-15T10:30:45.123456Z");
        Object result = converter.convertToAvro(instant, field, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof Long);

        // Verify microsecond precision
        long expectedMicros = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1000;
        assertEquals(expectedMicros, result);
    }

    @Test
    void testConvertToAvro_Timestamp_TimestampMicros() {
        Schema timestampSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema.Field field = new Schema.Field("test_timestamp_micros", timestampSchema);

        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45.123456");
        Object result = converter.convertToAvro(timestamp, field, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof Long);

        // Verify microsecond precision
        Instant instant = timestamp.toInstant();
        long expectedMicros = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1000;
        assertEquals(expectedMicros, result);
    }

    @Test
    void testConvertToJdbc_LongMicros_ToDatetime2() {
        Instant expected = Instant.parse("2024-01-15T10:30:45.123456Z");
        long micros = expected.getEpochSecond() * 1_000_000 + expected.getNano() / 1000;

        ColumnMetadata metadata = new ColumnMetadata("datetime2_col", Types.TIMESTAMP, "DATETIME2", 0, 6, true);
        Object result = converter.convertToJdbc(micros, metadata, TEST_JOB_ID);

        assertTrue(result instanceof Timestamp);
        Timestamp timestamp = (Timestamp) result;

        // Verify microsecond precision (allow 1 microsecond tolerance)
        long actualMicros = timestamp.toInstant().getEpochSecond() * 1_000_000
                + timestamp.toInstant().getNano() / 1000;
        assertEquals(micros, actualMicros);
    }

    @Test
    void testConvertToJdbc_LongMillis_ToDatetime2_WithLowScale() {
        Instant expected = Instant.parse("2024-01-15T10:30:45.123Z");
        long millis = expected.toEpochMilli();

        // scale=3 indicates milliseconds
        ColumnMetadata metadata = new ColumnMetadata("datetime2_col", Types.TIMESTAMP, "DATETIME2", 0, 3, true);
        Object result = converter.convertToJdbc(millis, metadata, TEST_JOB_ID);

        assertTrue(result instanceof Timestamp);
        Timestamp timestamp = (Timestamp) result;

        // Verify millisecond precision
        assertEquals(millis, timestamp.toInstant().toEpochMilli());
    }

    @Test
    void testConvertToAvro_Instant_TimestampMillis_StillWorks() {
        // Verify existing timestamp-millis functionality is not broken
        Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema.Field field = new Schema.Field("test_timestamp_millis", timestampSchema);

        Instant instant = Instant.parse("2024-01-15T10:30:45.123Z");
        Object result = converter.convertToAvro(instant, field, TEST_JOB_ID);

        assertNotNull(result);
        assertTrue(result instanceof Long);

        // Should still use milliseconds
        assertEquals(instant.toEpochMilli(), result);
    }
}
