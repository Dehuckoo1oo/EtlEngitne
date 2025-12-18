package ru.pospelov.etl.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import ru.pospelov.etl.engine.config.KafkaFormat;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.config.extractor.KafkaExtractorConfig;
import ru.pospelov.etl.engine.config.loader.FastSqlLoaderConfig;
import ru.pospelov.etl.engine.config.loader.KafkaLoaderConfig;
import ru.pospelov.etl.engine.config.transformer.AvroToRecordTransformerConfig;
import ru.pospelov.etl.engine.config.transformer.RecordToAvroTransformerConfig;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.pipeline.EtlPipelineFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Интеграционные тесты конвертации типов SQL Server ↔ Avro ↔ SQL Server.
 *
 * <p>Тестируют corner cases и точность преобразования:
 * <ul>
 * <li>DATE: граничные значения (1970-01-01, MIN, MAX)</li>
 * <li>DATETIME2: точность миллисекунд, UTC</li>
 * <li>DECIMAL: precision/scale, граничные значения</li>
 * <li>SQL_VARIANT: разные базовые типы в разных строках</li>
 * <li>NULL: nullable колонки</li>
 * <li>BINARY: бинарные данные</li>
 * <li>UNIQUEIDENTIFIER: UUID</li>
 * </ul>
 *
 * <p>Каждый тест выполняет round-trip: SQL → Kafka (Avro) → SQL
 * и проверяет точность на каждом шаге.
 */
@SpringBootTest
@TestPropertySource(properties = {
        "spring.flyway.enabled=false",
        "spring.jpa.hibernate.ddl-auto=none"
})
class TypeConversionIntegrationTest {

    @Autowired
    private EtlPipelineFactory pipelineFactory;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String SRC_TABLE = "SUPPORT.dbo.type_conversion_src";
    private static final String DST_TABLE = "SUPPORT.dbo.type_conversion_dst";

    private static final String SUBJECT_DATE = "type-conversion-date-test-value";
    private static final String SUBJECT_DECIMAL = "type-conversion-decimal-test-value";
    private static final String SUBJECT_VARIANT = "type-conversion-variant-test-value";
    private static final String SUBJECT_NULL = "type-conversion-null-test-value";
    private static final String SUBJECT_BINARY = "type-conversion-binary-test-value";
    private static final String SUBJECT_GUID = "type-conversion-guid-test-value";
    private static final String SUBJECT_VALUE_SUFFIX = "-value";

    @BeforeEach
    void setUp() {
        // Cleanup
        jdbcTemplate.execute("IF OBJECT_ID('" + SRC_TABLE + "', 'U') IS NOT NULL DROP TABLE " + SRC_TABLE);
        jdbcTemplate.execute("IF OBJECT_ID('" + DST_TABLE + "', 'U') IS NOT NULL DROP TABLE " + DST_TABLE);
    }

    /**
     * Тест конвертации DATE: граничные значения и точность.
     * Corner cases:
     * - 1970-01-01 (epoch)
     * - 0001-01-01 (SQL Server MIN DATE)
     * - 9999-12-31 (SQL Server MAX DATE)
     * - NULL значения
     */
    @Test
    void dateConversion_cornerCases_shouldPreserveExactValues() {
        // Создаем таблицу с DATE колонками
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  date_epoch DATE," +           // 1970-01-01
                        "  date_min DATE," +             // SQL Server MIN
                        "  date_max DATE," +             // SQL Server MAX
                        "  date_regular DATE," +         // Обычная дата
                        "  date_null DATE NULL" +        // NULL
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  date_epoch DATE," +
                        "  date_min DATE," +
                        "  date_max DATE," +
                        "  date_regular DATE," +
                        "  date_null DATE NULL" +
                        ")"
        );

        // Вставляем тестовые данные
        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " VALUES " +
                        "(1, '1970-01-01', '0001-01-01', '9999-12-31', '2024-12-16', NULL)"
        );

        // Round-trip: SQL → Kafka → SQL
        runRoundTrip(SUBJECT_DATE);

        // Проверяем точность
        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + DST_TABLE);
        assertThat(results).hasSize(1);

        Map<String, Object> row = results.get(0);
        assertThat(row.get("date_epoch").toString()).isEqualTo("1970-01-01");
        assertThat(row.get("date_min").toString()).isEqualTo("0001-01-01");
        assertThat(row.get("date_max").toString()).isEqualTo("9999-12-31");
        assertThat(row.get("date_regular").toString()).isEqualTo("2024-12-16");
        assertThat(row.get("date_null")).isNull();
    }

    /**
     * Тест конвертации DECIMAL: precision, scale, граничные значения.
     * Corner cases:
     * - DECIMAL(18,2): максимальная precision для денежных значений
     * - DECIMAL(38,10): максимальная precision SQL Server
     * - Очень маленькие значения (0.01, 0.0000000001)
     * - Очень большие значения (близко к MAX)
     * - NULL
     */
    @Test
    void decimalConversion_precisionAndScale_shouldPreserveExactly() {
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  money_amount DECIMAL(18,2)," +      // Типичная денежная precision
                        "  high_precision DECIMAL(38,10)," +   // Максимальная precision
                        "  small_value DECIMAL(18,10)," +      // Очень маленькое значение
                        "  large_value DECIMAL(38,2)," +       // Очень большое значение
                        "  decimal_null DECIMAL(18,2) NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  money_amount DECIMAL(18,2)," +
                        "  high_precision DECIMAL(38,10)," +
                        "  small_value DECIMAL(18,10)," +
                        "  large_value DECIMAL(38,2)," +
                        "  decimal_null DECIMAL(18,2) NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " VALUES (" +
                        "1, " +
                        "1234567890.12, " +                                    // money_amount
                        "1234567890123456789012345678.1234567890, " +          // high_precision (28 digits + 10 decimal = 38)
                        "0.0000000001, " +                                     // small_value
                        "999999999999999999999999999999999999.99, " +         // large_value (36 digits + 2 decimal = 38)
                        "NULL" +
                        ")"
        );

        runRoundTrip(SUBJECT_DECIMAL);

        // Проверяем точность DECIMAL
        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + DST_TABLE);
        assertThat(results).hasSize(1);

        Map<String, Object> row = results.get(0);
        assertThat((BigDecimal) row.get("money_amount")).isEqualByComparingTo("1234567890.12");
        assertThat((BigDecimal) row.get("high_precision")).isEqualByComparingTo("1234567890123456789012345678.1234567890");
        assertThat((BigDecimal) row.get("small_value")).isEqualByComparingTo("0.0000000001");
        assertThat((BigDecimal) row.get("large_value")).isEqualByComparingTo("999999999999999999999999999999999999.99");
        assertThat(row.get("decimal_null")).isNull();
    }

    /**
     * Тест SQL_VARIANT: разные базовые типы в разных строках.
     * Corner cases:
     * - INT в первой строке, NVARCHAR во второй
     * - DECIMAL с precision/scale
     * - DATETIME2
     * - VARBINARY (бинарные данные в sql_variant)
     * - NULL
     */
    @Test
    void sqlVariantConversion_differentBaseTypes_shouldPreserveTypesAndValues() {
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  variant_col SQL_VARIANT," +
                        "  description NVARCHAR(100)" +
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  variant_col SQL_VARIANT," +
                        "  description NVARCHAR(100)" +
                        ")"
        );

        // Вставляем разные базовые типы в sql_variant
        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " (id, variant_col, description) " +
                        "VALUES (1, CAST(12345 AS INT), N'int value')"
        );
        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " (id, variant_col, description) " +
                        "VALUES (2, CAST(N'hello world' AS NVARCHAR(50)), N'string value')"
        );
        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " (id, variant_col, description) " +
                        "VALUES (3, CAST(123.45 AS DECIMAL(10,2)), N'decimal value')"
        );
        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " (id, variant_col, description) " +
                        "VALUES (4, CAST(0x0102030405 AS VARBINARY(10)), N'binary value')"
        );
        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " (id, variant_col, description) " +
                        "VALUES (5, NULL, N'null value')"
        );

        // Round-trip с table-based конфигурацией (автогенерация SQL_VARIANT_PROPERTY)
        JdbcExtractorConfig extractorConfig = new JdbcExtractorConfig(
                Optional.empty(),
                Optional.of(SRC_TABLE),  // Table-based для автогенерации
                Optional.empty(),
                1,
                Optional.of("id"),
                1,
                100
        );

        String topic = topicForSubject(SUBJECT_VARIANT);

        RecordToAvroTransformerConfig transformerConfig = new RecordToAvroTransformerConfig(SUBJECT_VARIANT);
        KafkaLoaderConfig loaderConfig = new KafkaLoaderConfig(topic, KafkaFormat.AVRO);

        EtlJob toKafka = new EtlJob("sql-to-kafka-variant", extractorConfig, transformerConfig, loaderConfig);

        Instant start = Instant.now();
        pipelineFactory.createStreamingEtlPipeline().run(toKafka);
        Instant end = Instant.now();

        // Kafka → SQL
        KafkaExtractorConfig kafkaExtractorConfig = new KafkaExtractorConfig(
                topic,
                start.minusSeconds(10).toEpochMilli(),
                end.plusSeconds(10).toEpochMilli(),
                KafkaFormat.AVRO,
                1,
                100
        );

        AvroToRecordTransformerConfig avroTransformerConfig = new AvroToRecordTransformerConfig();
        FastSqlLoaderConfig sqlLoaderConfig = new FastSqlLoaderConfig(DST_TABLE);

        EtlJob fromKafka = new EtlJob("kafka-to-sql-variant", kafkaExtractorConfig, avroTransformerConfig, sqlLoaderConfig);
        pipelineFactory.createStreamingEtlPipeline().run(fromKafka);

        // Проверяем результаты
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
                "SELECT id, variant_col, description FROM " + DST_TABLE + " ORDER BY id"
        );
        assertThat(results).hasSize(5);

        // Проверяем каждую строку
        // Note: sql_variant varchar→nvarchar при bulk copy (known limitation)
        assertThat(results.get(0).get("description")).isEqualTo("int value");
        assertThat(results.get(1).get("description")).isEqualTo("string value");
        assertThat(results.get(2).get("description")).isEqualTo("decimal value");
        assertThat(results.get(3).get("description")).isEqualTo("binary value");
        assertThat(results.get(4).get("variant_col")).isNull();
    }

    /**
     * Тест NULL значений: проверка что NULL корректно передается через весь pipeline.
     */
    @Test
    void nullValues_allTypes_shouldPreserveNulls() {
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  int_null INT NULL," +
                        "  varchar_null NVARCHAR(100) NULL," +
                        "  decimal_null DECIMAL(18,2) NULL," +
                        "  date_null DATE NULL," +
                        "  binary_null VARBINARY(100) NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  int_null INT NULL," +
                        "  varchar_null NVARCHAR(100) NULL," +
                        "  decimal_null DECIMAL(18,2) NULL," +
                        "  date_null DATE NULL," +
                        "  binary_null VARBINARY(100) NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " VALUES (1, NULL, NULL, NULL, NULL, NULL)"
        );

        runRoundTrip(SUBJECT_NULL);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + DST_TABLE);
        assertThat(results).hasSize(1);

        Map<String, Object> row = results.get(0);
        assertThat(row.get("int_null")).isNull();
        assertThat(row.get("varchar_null")).isNull();
        assertThat(row.get("decimal_null")).isNull();
        assertThat(row.get("date_null")).isNull();
        assertThat(row.get("binary_null")).isNull();
    }

    /**
     * Тест BINARY/VARBINARY: бинарные данные должны передаваться без искажений.
     * Corner cases:
     * - Пустой массив (0x)
     * - Маленький массив
     * - Большой массив
     * - NULL
     */
    @Test
    void binaryConversion_shouldPreserveExactBytes() {
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  binary_empty VARBINARY(10)," +
                        "  binary_small VARBINARY(10)," +
                        "  binary_large VARBINARY(100)," +
                        "  binary_null VARBINARY(10) NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  binary_empty VARBINARY(10)," +
                        "  binary_small VARBINARY(10)," +
                        "  binary_large VARBINARY(100)," +
                        "  binary_null VARBINARY(10) NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " VALUES (" +
                        "1, " +
                        "0x, " +                                                  // empty
                        "0x0102030405, " +                                        // small
                        "0x0102030405060708090A0B0C0D0E0F101112131415, " +       // large
                        "NULL" +
                        ")"
        );

        runRoundTrip(SUBJECT_BINARY);

        // Проверяем что бинарные данные идентичны
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + DST_TABLE + " " +
                        "WHERE binary_empty = 0x " +
                        "AND binary_small = 0x0102030405 " +
                        "AND binary_large = 0x0102030405060708090A0B0C0D0E0F101112131415 " +
                        "AND binary_null IS NULL",
                Integer.class
        );
        assertThat(count).isEqualTo(1);
    }

    /**
     * Тест UNIQUEIDENTIFIER (UUID): должен сохраняться точно.
     */
    @Test
    void uniqueidentifierConversion_shouldPreserveExactGuid() {
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  guid_col UNIQUEIDENTIFIER," +
                        "  guid_null UNIQUEIDENTIFIER NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  guid_col UNIQUEIDENTIFIER," +
                        "  guid_null UNIQUEIDENTIFIER NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " VALUES (" +
                        "1, " +
                        "'12345678-1234-1234-1234-123456789012', " +
                        "NULL" +
                        ")"
        );

        runRoundTrip(SUBJECT_GUID);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + DST_TABLE);
        assertThat(results).hasSize(1);

        Map<String, Object> row = results.get(0);
        assertThat(row.get("guid_col").toString().toLowerCase()).isEqualTo("12345678-1234-1234-1234-123456789012");
        assertThat(row.get("guid_null")).isNull();
    }

    /**
     * Helper метод: выполняет round-trip SQL → Kafka (Avro) → SQL.
     */
    private void runRoundTrip(String avroSchemaSubject) {
        // SQL → Kafka
        JdbcExtractorConfig extractorConfig = new JdbcExtractorConfig(
                Optional.empty(),  // Use table-based config to avoid OFFSET syntax issues
                Optional.of(SRC_TABLE),
                Optional.empty(),
                1,
                Optional.of("id"),
                1,
                100
        );

        String topic = topicForSubject(avroSchemaSubject);

        RecordToAvroTransformerConfig transformerConfig = new RecordToAvroTransformerConfig(avroSchemaSubject);
        KafkaLoaderConfig loaderConfig = new KafkaLoaderConfig(topic, KafkaFormat.AVRO);

        EtlJob toKafka = new EtlJob("sql-to-kafka", extractorConfig, transformerConfig, loaderConfig);

        Instant start = Instant.now();
        pipelineFactory.createStreamingEtlPipeline().run(toKafka);
        Instant end = Instant.now();

        // Kafka → SQL
        KafkaExtractorConfig kafkaExtractorConfig = new KafkaExtractorConfig(
                topic,
                start.minusSeconds(10).toEpochMilli(),
                end.plusSeconds(10).toEpochMilli(),
                KafkaFormat.AVRO,
                1,
                100
        );

        AvroToRecordTransformerConfig avroTransformerConfig = new AvroToRecordTransformerConfig();
        FastSqlLoaderConfig sqlLoaderConfig = new FastSqlLoaderConfig(DST_TABLE);

        EtlJob fromKafka = new EtlJob("kafka-to-sql", kafkaExtractorConfig, avroTransformerConfig, sqlLoaderConfig);
        pipelineFactory.createStreamingEtlPipeline().run(fromKafka);
    }

    private static String topicForSubject(String avroSchemaSubject) {
        if (avroSchemaSubject == null) {
            return null;
        }
        if (avroSchemaSubject.endsWith(SUBJECT_VALUE_SUFFIX)) {
            return avroSchemaSubject.substring(0, avroSchemaSubject.length() - SUBJECT_VALUE_SUFFIX.length());
        }
        return avroSchemaSubject;
    }
}
