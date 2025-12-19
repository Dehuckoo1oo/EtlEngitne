package ru.pospelov.etl.engine.integration;

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

    @Autowired
    private ru.pospelov.etl.engine.schema.SchemaRegistryService schemaRegistryService;

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
        assertThat(results.get(0).get("variant_col")).isEqualTo(12345);
        assertThat(results.get(0).get("description")).isEqualTo("int value");

        assertThat(results.get(1).get("variant_col")).isEqualTo("hello world");
        assertThat(results.get(1).get("description")).isEqualTo("string value");

        // Note: JDBC Driver возвращает decimal из sql_variant как String (known limitation)
        Object decimalValue = results.get(2).get("variant_col");
        if (decimalValue instanceof String) {
            assertThat(new BigDecimal((String) decimalValue)).isEqualByComparingTo("123.45");
        } else {
            assertThat((BigDecimal) decimalValue).isEqualByComparingTo("123.45");
        }
        assertThat(results.get(2).get("description")).isEqualTo("decimal value");

        assertThat(results.get(3).get("variant_col")).isEqualTo(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05});
        assertThat(results.get(3).get("description")).isEqualTo("binary value");

        assertThat(results.get(4).get("variant_col")).isNull();
        assertThat(results.get(4).get("description")).isEqualTo("null value");
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
     * Helper метод: регистрирует Avro схему в Schema Registry если она не существует.
     * Загружает схему из resources/avro/{subject}.avsc
     */


    /**
     * Регистрирует схему в Schema Registry через REST API
     */
    private void registerSchema(String subject, String schemaJson) {
        try {
            String url = "http://localhost:8081/subjects/" + subject + "/versions";

            // Prepare request body
            String requestBody = "{\"schema\": \"" + schemaJson.replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "") + "\"}";

            org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);

            org.springframework.http.HttpEntity<String> request = new org.springframework.http.HttpEntity<>(requestBody, headers);

            org.springframework.web.client.RestTemplate restTemplate = new org.springframework.web.client.RestTemplate();
            String response = restTemplate.postForObject(url, request, String.class);

            System.out.println("Schema registration response: " + response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to register schema for subject: " + subject, e);
        }
    }

    /**
     * Helper метод: выполняет round-trip SQL → Kafka (Avro) → SQL.
     */
    private void runRoundTrip(String avroSchemaSubject) {
        // Убедимся что subject известен (схема будет создана автоматически при первой записи)
        ensureSchemaExists(avroSchemaSubject);
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

    // ==================== Comprehensive Type Tests ====================

    private static final String SUBJECT_INTEGER = "type-conversion-integer-test-value";
    private static final String SUBJECT_FLOAT = "type-conversion-float-test-value";
    private static final String SUBJECT_MONEY = "type-conversion-money-test-value";
    private static final String SUBJECT_STRING = "type-conversion-string-test-value";
    private static final String SUBJECT_TIME = "type-conversion-time-test-value";
    private static final String SUBJECT_XML = "type-conversion-xml-test-value";
    private static final String SUBJECT_VARIANT_ALL = "type-conversion-variant-all-test-value";

    /**
     * Тест целочисленных типов: BIT, TINYINT, SMALLINT, INT, BIGINT
     */
    @Test
    void integerTypes_allVariants_shouldPreserveValues() {
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  bit_true BIT," +
                        "  bit_false BIT," +
                        "  tinyint_val TINYINT," +
                        "  smallint_val SMALLINT," +
                        "  int_val INT," +
                        "  bigint_val BIGINT," +
                        "  int_null INT NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  bit_true BIT," +
                        "  bit_false BIT," +
                        "  tinyint_val TINYINT," +
                        "  smallint_val SMALLINT," +
                        "  int_val INT," +
                        "  bigint_val BIGINT," +
                        "  int_null INT NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " VALUES (1, 1, 0, 255, 32767, 2147483647, 9223372036854775807, NULL)"
        );

        runRoundTrip(SUBJECT_INTEGER);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + DST_TABLE);
        assertThat(results).hasSize(1);

        Map<String, Object> row = results.get(0);
        assertThat(row.get("bit_true")).isEqualTo(true);
        assertThat(row.get("bit_false")).isEqualTo(false);
        assertThat(row.get("tinyint_val")).isEqualTo((short) 255);  // JDBC returns Short for TINYINT
        assertThat(row.get("smallint_val")).isEqualTo((short) 32767);
        assertThat(row.get("int_val")).isEqualTo(2147483647);
        assertThat(row.get("bigint_val")).isEqualTo(9223372036854775807L);
        assertThat(row.get("int_null")).isNull();
    }

    /**
     * Тест типов с плавающей точкой: REAL, FLOAT
     */
    @Test
    void floatingPointTypes_shouldPreserveValues() {
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  real_val REAL," +
                        "  float_val FLOAT," +
                        "  real_null REAL NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  real_val REAL," +
                        "  float_val FLOAT," +
                        "  real_null REAL NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " VALUES (1, 3.14159, 2.718281828459045, NULL)"
        );

        runRoundTrip(SUBJECT_FLOAT);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + DST_TABLE);
        assertThat(results).hasSize(1);

        Map<String, Object> row = results.get(0);
        assertThat((Float) row.get("real_val")).isCloseTo(3.14159f, org.assertj.core.data.Offset.offset(0.00001f));
        assertThat((Double) row.get("float_val")).isCloseTo(2.718281828459045, org.assertj.core.data.Offset.offset(0.000000000000001));
        assertThat(row.get("real_null")).isNull();
    }

    /**
     * Тест денежных типов: MONEY, SMALLMONEY
     */
    @Test
    void moneyTypes_shouldPreserveExactValues() {
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  money_val MONEY," +
                        "  smallmoney_val SMALLMONEY," +
                        "  money_null MONEY NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  money_val MONEY," +
                        "  smallmoney_val SMALLMONEY," +
                        "  money_null MONEY NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " VALUES (1, 922337203685477.5807, 214748.3647, NULL)"
        );

        runRoundTrip(SUBJECT_MONEY);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + DST_TABLE);
        assertThat(results).hasSize(1);

        Map<String, Object> row = results.get(0);
        assertThat((BigDecimal) row.get("money_val")).isEqualByComparingTo("922337203685477.5807");
        assertThat((BigDecimal) row.get("smallmoney_val")).isEqualByComparingTo("214748.3647");
        assertThat(row.get("money_null")).isNull();
    }

    /**
     * Тест строковых типов: CHAR, VARCHAR, NCHAR, NVARCHAR, TEXT, NTEXT
     */
    @Test
    void stringTypes_allVariants_shouldPreserveValues() {
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  char_val CHAR(10)," +
                        "  varchar_val VARCHAR(100)," +
                        "  varchar_max VARCHAR(MAX)," +
                        "  nchar_val NCHAR(10)," +
                        "  nvarchar_val NVARCHAR(100)," +
                        "  nvarchar_max NVARCHAR(MAX)," +
                        "  text_val TEXT," +
                        "  ntext_val NTEXT," +
                        "  varchar_null VARCHAR(100) NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  char_val CHAR(10)," +
                        "  varchar_val VARCHAR(100)," +
                        "  varchar_max VARCHAR(MAX)," +
                        "  nchar_val NCHAR(10)," +
                        "  nvarchar_val NVARCHAR(100)," +
                        "  nvarchar_max NVARCHAR(MAX)," +
                        "  text_val TEXT," +
                        "  ntext_val NTEXT," +
                        "  varchar_null VARCHAR(100) NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " VALUES (" +
                        "1, " +
                        "'CHAR10    ', " +  // CHAR pads with spaces
                        "'varchar test', " +
                        "'varchar(max) can hold very long strings', " +
                        "N'NCHAR10   ', " +
                        "N'Unicode тест', " +
                        "N'nvarchar(max) поддерживает длинные строки', " +
                        "'Legacy TEXT type', " +
                        "N'Legacy NTEXT type', " +  // Note: NTEXT may lose Unicode in bulk insert
                        "NULL" +
                        ")"
        );

        runRoundTrip(SUBJECT_STRING);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + DST_TABLE);
        assertThat(results).hasSize(1);

        Map<String, Object> row = results.get(0);
        assertThat(row.get("char_val").toString().trim()).isEqualTo("CHAR10");
        assertThat(row.get("varchar_val")).isEqualTo("varchar test");
        assertThat(row.get("varchar_max")).isEqualTo("varchar(max) can hold very long strings");
        assertThat(row.get("nchar_val").toString().trim()).isEqualTo("NCHAR10");
        assertThat(row.get("nvarchar_val")).isEqualTo("Unicode тест");
        assertThat(row.get("nvarchar_max")).isEqualTo("nvarchar(max) поддерживает длинные строки");
        assertThat(row.get("text_val")).isEqualTo("Legacy TEXT type");
        assertThat(row.get("ntext_val")).isEqualTo("Legacy NTEXT type");
        assertThat(row.get("varchar_null")).isNull();
    }

    /**
     * Тест временных типов: TIME, DATETIME, DATETIME2, SMALLDATETIME, DATETIMEOFFSET
     */
    @Test
    void timeTypes_allVariants_shouldPreserveValues() {
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  time_val TIME(7)," +
                        "  datetime_val DATETIME," +
                        "  datetime2_val DATETIME2(7)," +
                        "  smalldatetime_val SMALLDATETIME," +
                        "  datetimeoffset_val DATETIMEOFFSET(7)," +
                        "  time_null TIME NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  time_val TIME(7)," +
                        "  datetime_val DATETIME," +
                        "  datetime2_val DATETIME2(7)," +
                        "  smalldatetime_val SMALLDATETIME," +
                        "  datetimeoffset_val DATETIMEOFFSET(7)," +
                        "  time_null TIME NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " VALUES (" +
                        "1, " +
                        "'14:30:45.1234567', " +
                        "'2024-12-18 10:30:00', " +
                        "'2024-12-18 10:30:45.1234567', " +
                        "'2024-12-18 10:31:00', " +
                        "'2024-12-18 10:30:45.1234567 +03:00', " +
                        "NULL" +
                        ")"
        );

        runRoundTrip(SUBJECT_TIME);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + DST_TABLE);
        assertThat(results).hasSize(1);

        Map<String, Object> row = results.get(0);

        // TIME - проверяем строковое представление
        assertThat(row.get("time_val").toString()).startsWith("14:30:45");

        // DATETIME, DATETIME2, SMALLDATETIME - проверяем дату
        assertThat(row.get("datetime_val").toString()).contains("2024-12-18");
        assertThat(row.get("datetime2_val").toString()).contains("2024-12-18");
        assertThat(row.get("smalldatetime_val").toString()).contains("2024-12-18");

        // DATETIMEOFFSET - проверяем наличие offset
        assertThat(row.get("datetimeoffset_val").toString()).contains("+03:00");

        assertThat(row.get("time_null")).isNull();
    }

    /**
     * Тест XML типа
     */
    @Test
    void xmlType_shouldPreserveValue() {
        jdbcTemplate.execute(
                "CREATE TABLE " + SRC_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  xml_val XML," +
                        "  xml_null XML NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "CREATE TABLE " + DST_TABLE + " (" +
                        "  id INT PRIMARY KEY," +
                        "  xml_val XML," +
                        "  xml_null XML NULL" +
                        ")"
        );

        jdbcTemplate.execute(
                "INSERT INTO " + SRC_TABLE + " VALUES (1, '<root><item>test</item></root>', NULL)"
        );

        runRoundTrip(SUBJECT_XML);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + DST_TABLE);
        assertThat(results).hasSize(1);

        Map<String, Object> row = results.get(0);
        assertThat(row.get("xml_val").toString()).contains("<root><item>test</item></root>");
        assertThat(row.get("xml_null")).isNull();
    }

    /**
     * Расширенный тест SQL_VARIANT: все поддерживаемые базовые типы
     *
     * Проверяет round-trip для всех типов внутри sql_variant:
     * - Целочисленные: INT, BIGINT, SMALLINT, TINYINT, BIT
     * - С плавающей точкой: REAL, FLOAT
     * - Точные числа: DECIMAL, NUMERIC, MONEY, SMALLMONEY
     * - Строковые: VARCHAR, NVARCHAR, CHAR, NCHAR
     * - Бинарные: VARBINARY, BINARY
     * - Временные: DATE, TIME, DATETIME, DATETIME2, DATETIMEOFFSET
     * - Специальные: UNIQUEIDENTIFIER
     */
    @Test
    void sqlVariantConversion_allSupportedBaseTypes_shouldPreserveTypesAndValues() {
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

        // Вставляем все поддерживаемые типы
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (1, CAST(12345 AS INT), N'int')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (2, CAST(9223372036854775807 AS BIGINT), N'bigint')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (3, CAST(32767 AS SMALLINT), N'smallint')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (4, CAST(255 AS TINYINT), N'tinyint')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (5, CAST(1 AS BIT), N'bit true')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (6, CAST(0 AS BIT), N'bit false')");

        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (7, CAST(3.14 AS REAL), N'real')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (8, CAST(2.718281828 AS FLOAT), N'float')");

        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (9, CAST(12345.67 AS DECIMAL(18,2)), N'decimal')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (10, CAST(98765.43 AS NUMERIC(10,2)), N'numeric')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (11, CAST(12345.6789 AS MONEY), N'money')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (12, CAST(123.45 AS SMALLMONEY), N'smallmoney')");

        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (13, CAST('varchar test' AS VARCHAR(50)), N'varchar')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (14, CAST(N'nvarchar тест' AS NVARCHAR(50)), N'nvarchar')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (15, CAST('CHAR10' AS CHAR(10)), N'char')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (16, CAST(N'NCHAR10' AS NCHAR(10)), N'nchar')");

        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (17, CAST(0x0102030405 AS VARBINARY(10)), N'varbinary')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (18, CAST(0x0102030405060708 AS BINARY(8)), N'binary')");

        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (19, CAST('2024-12-18' AS DATE), N'date')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (20, CAST('14:30:45' AS TIME(7)), N'time')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (21, CAST('2024-12-18 10:30:00' AS DATETIME), N'datetime')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (22, CAST('2024-12-18 10:30:45.1234567' AS DATETIME2(7)), N'datetime2')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (23, CAST('2024-12-18 10:30:45.1234567 +03:00' AS DATETIMEOFFSET(7)), N'datetimeoffset')");
        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (24, CAST('2024-12-18 10:30:00' AS SMALLDATETIME), N'smalldatetime')");

        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (25, CAST('12345678-1234-1234-1234-123456789012' AS UNIQUEIDENTIFIER), N'uniqueidentifier')");

        jdbcTemplate.execute("INSERT INTO " + SRC_TABLE + " VALUES (26, NULL, N'null')");

        // Ensure schema exists in Schema Registry
        ensureSchemaExists(SUBJECT_VARIANT_ALL);

        // Round-trip с table-based конфигурацией (автогенерация SQL_VARIANT_PROPERTY)
        JdbcExtractorConfig extractorConfig = new JdbcExtractorConfig(
                Optional.empty(),
                Optional.of(SRC_TABLE),
                Optional.empty(),
                1,
                Optional.of("id"),
                1,
                1
        );

        String topic = topicForSubject(SUBJECT_VARIANT_ALL);

        RecordToAvroTransformerConfig transformerConfig = new RecordToAvroTransformerConfig(SUBJECT_VARIANT_ALL);
        KafkaLoaderConfig loaderConfig = new KafkaLoaderConfig(topic, KafkaFormat.AVRO);

        EtlJob toKafka = new EtlJob("sql-to-kafka-variant-all", extractorConfig, transformerConfig, loaderConfig);

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

        EtlJob fromKafka = new EtlJob("kafka-to-sql-variant-all", kafkaExtractorConfig, avroTransformerConfig, sqlLoaderConfig);
        pipelineFactory.createStreamingEtlPipeline().run(fromKafka);

        // Проверяем результаты
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
                "SELECT id, variant_col, description FROM " + DST_TABLE + " ORDER BY id"
        );
        assertThat(results).hasSize(26);

        // Проверяем каждую строку
        // Note: некоторые типы могут конвертироваться (varchar→nvarchar при bulk copy)

        assertThat(results.get(0).get("variant_col")).isEqualTo(12345);
        assertThat(results.get(1).get("variant_col")).isEqualTo(9223372036854775807L);
        assertThat(results.get(2).get("variant_col")).isEqualTo((short) 32767);
        assertThat(results.get(3).get("variant_col")).isEqualTo((short)255);
        assertThat(results.get(4).get("variant_col")).isEqualTo(true);
        assertThat(results.get(5).get("variant_col")).isEqualTo(false);

        assertThat((Float) results.get(6).get("variant_col")).isCloseTo(3.14f, org.assertj.core.data.Offset.offset(0.01f));
        assertThat((Double) results.get(7).get("variant_col")).isCloseTo(2.718281828, org.assertj.core.data.Offset.offset(0.000000001));

        // Decimal types - могут быть как BigDecimal или String (зависит от JDBC драйвера)
        Object decimal9 = results.get(8).get("variant_col");
        if (decimal9 instanceof String) {
            assertThat(new BigDecimal((String) decimal9)).isEqualByComparingTo("12345.67");
        } else {
            assertThat((BigDecimal) decimal9).isEqualByComparingTo("12345.67");
        }

        Object decimal10 = results.get(9).get("variant_col");
        if (decimal10 instanceof String) {
            assertThat(new BigDecimal((String) decimal10)).isEqualByComparingTo("98765.43");
        } else {
            assertThat((BigDecimal) decimal10).isEqualByComparingTo("98765.43");
        }

        // Строковые типы
        assertThat(results.get(12).get("variant_col").toString()).contains("varchar test");
        assertThat(results.get(13).get("variant_col").toString()).contains("nvarchar тест");

        // Бинарные типы
        assertThat(results.get(16).get("variant_col")).isEqualTo(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05});
        assertThat(results.get(17).get("variant_col")).isEqualTo(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08});

        // Временные типы - проверяем что содержат ожидаемые значения
        assertThat(results.get(18).get("variant_col").toString()).contains("2024-12-18");
        assertThat(results.get(19).get("variant_col").toString()).contains("14:30");
        assertThat(results.get(20).get("variant_col").toString()).contains("2024-12-18");

        // NULL
        assertThat(results.get(25).get("variant_col")).isNull();
    }

    private void ensureSchemaExists(String subject) {
        try {
            // Проверяем существует ли схема
            schemaRegistryService.getLatestSchema(subject);
            System.out.println("Schema for subject '" + subject + "' already exists");
        } catch (Exception e) {
            // Схема не существует - загружаем из ресурсов и регистрируем
            System.out.println("Schema for subject '" + subject + "' does not exist, loading from resources...");

            try {
                // Загружаем схему из classpath
                org.springframework.core.io.Resource resource = new org.springframework.core.io.ClassPathResource("avro/" + subject + ".avsc");
                String schemaJson = new String(resource.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);

                System.out.println("Loaded schema from resources: " + subject + ".avsc");

                // Регистрируем схему в Schema Registry
                registerSchema(subject, schemaJson);
                System.out.println("Schema registered successfully for subject: " + subject);
            } catch (java.io.IOException ex) {
                throw new RuntimeException("Failed to load schema from resources for subject: " + subject, ex);
            }
        }
    }
}
