package ru.pospelov.etl.engine.api.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.pospelov.etl.api.job.entity.JobEntity;
import ru.pospelov.etl.api.job.mapper.JobMapper;
import ru.pospelov.etl.engine.config.KafkaFormat;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.config.extractor.KafkaExtractorConfig;
import ru.pospelov.etl.engine.config.loader.FastSqlLoaderConfig;
import ru.pospelov.etl.engine.config.loader.JdbcLoaderConfig;
import ru.pospelov.etl.engine.config.loader.KafkaLoaderConfig;
import ru.pospelov.etl.engine.config.transformer.AvroToRecordTransformerConfig;
import ru.pospelov.etl.engine.config.transformer.NoopTransformerConfig;
import ru.pospelov.etl.engine.config.transformer.RecordToAvroTransformerConfig;
import ru.pospelov.etl.engine.model.EtlJob;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit-тесты для JobMapper с type-safe конфигурацией
 */
class JobMapperTest {

    private JobMapper jobMapper;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        jobMapper = new JobMapper(objectMapper);
    }

    @Test
    void toEntity_shouldConvertEtlJobToJobEntity() {
        // Given
        JdbcExtractorConfig extractorConfig = new JdbcExtractorConfig(
                Optional.of("SELECT * FROM test_table"),
                Optional.empty(), // table
                Optional.empty(), // partitionColumn
                1,
                Optional.empty(), // keyColumn
                4,
                1000
        );

        NoopTransformerConfig transformerConfig = new NoopTransformerConfig();

        KafkaLoaderConfig loaderConfig = new KafkaLoaderConfig(
                "target-topic",
                KafkaFormat.AVRO
        );

        EtlJob etlJob = new EtlJob(
                "test-job-1",
                extractorConfig,
                transformerConfig,
                loaderConfig
        );

        // When
        JobEntity entity = jobMapper.toEntity(etlJob);

        // Then
        assertNotNull(entity);
        assertEquals("test-job-1", entity.getId());
        assertEquals("test-job-1", entity.getName()); // По умолчанию name = id
        assertEquals("sql", entity.getExtractorType());
        assertEquals("noop", entity.getTransformerType());
        assertEquals("kafka", entity.getLoaderType());
        assertEquals("ACTIVE", entity.getStatus());

        // Проверяем, что конфигурации сохранены как JSON
        assertNotNull(entity.getExtractorConfig());
        assertNotNull(entity.getTransformerConfig());
        assertNotNull(entity.getLoaderConfig());
        assertTrue(entity.getExtractorConfig().contains("SELECT * FROM test_table"));
        assertTrue(entity.getLoaderConfig().contains("target-topic"));
    }

    @Test
    void toEntity_withMetadata_shouldIncludeCreatedByAndDescription() {
        // Given
        KafkaExtractorConfig extractorConfig = new KafkaExtractorConfig(
                "source-topic",
                System.currentTimeMillis(),
                System.currentTimeMillis() + 1000,
                KafkaFormat.AVRO,
                4,
                1000
        );

        NoopTransformerConfig transformerConfig = new NoopTransformerConfig();

        JdbcLoaderConfig loaderConfig = new JdbcLoaderConfig("target_table", 1000);

        EtlJob etlJob = new EtlJob("test-job-2", extractorConfig, transformerConfig, loaderConfig);

        // When
        JobEntity entity = jobMapper.toEntity(etlJob, "user123", "Test description");

        // Then
        assertNotNull(entity);
        assertEquals("test-job-2", entity.getId());
        assertEquals("user123", entity.getCreatedBy());
        assertEquals("Test description", entity.getDescription());
    }

    @Test
    void toEtlJob_shouldConvertJobEntityToEtlJob() throws Exception {
        // Given
        JdbcExtractorConfig extractorConfig = new JdbcExtractorConfig(
                Optional.of("SELECT * FROM orders"),
                Optional.empty(), // table
                Optional.empty(), // partitionColumn
                1,
                Optional.empty(), // keyColumn
                8,
                1000
        );

        NoopTransformerConfig transformerConfig = new NoopTransformerConfig();

        KafkaLoaderConfig loaderConfig = new KafkaLoaderConfig(
                "kafka-topic",
                KafkaFormat.AVRO
        );

        String extractorJson = objectMapper.writeValueAsString(extractorConfig);
        String transformerJson = objectMapper.writeValueAsString(transformerConfig);
        String loaderJson = objectMapper.writeValueAsString(loaderConfig);

        JobEntity entity = JobEntity.builder()
                .id("test-job-3")
                .name("Test Job 3")
                .extractorType("sql")
                .extractorConfig(extractorJson)
                .transformerType("noop")
                .transformerConfig(transformerJson)
                .loaderType("kafka")
                .loaderConfig(loaderJson)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .createdBy("admin")
                .status("ACTIVE")
                .description("Test job description")
                .build();

        // When
        EtlJob etlJob = jobMapper.toEtlJob(entity);

        // Then
        assertNotNull(etlJob);
        assertEquals("test-job-3", etlJob.jobId());

        // Проверяем конфигурации
        assertTrue(etlJob.extractorConfig() instanceof JdbcExtractorConfig);
        JdbcExtractorConfig jdbcConfig = (JdbcExtractorConfig) etlJob.extractorConfig();
        assertEquals("SELECT * FROM orders", jdbcConfig.sqlQuery().orElse(""));
        assertEquals(8, jdbcConfig.threads());

        assertTrue(etlJob.transformerConfig() instanceof NoopTransformerConfig);

        assertTrue(etlJob.loaderConfig() instanceof KafkaLoaderConfig);
        KafkaLoaderConfig kafkaConfig = (KafkaLoaderConfig) etlJob.loaderConfig();
        assertEquals("kafka-topic", kafkaConfig.topic());
    }

    @Test
    void toEtlJob_withInvalidJson_shouldThrowException() {
        // Given
        JobEntity entity = JobEntity.builder()
                .id("test-job-4")
                .extractorType("sql")
                .extractorConfig("invalid json {{{")
                .transformerType("noop")
                .transformerConfig("{}")
                .loaderType("kafka")
                .loaderConfig("{}")
                .build();

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> jobMapper.toEtlJob(entity));
    }

    @Test
    void updateEntity_shouldUpdateOnlyRelevantFields() throws Exception {
        // Given
        JdbcExtractorConfig originalExtractorConfig = new JdbcExtractorConfig(
                Optional.of("SELECT * FROM old_table"),
                Optional.empty(), // table
                Optional.empty(), // partitionColumn
                1,
                Optional.empty(), // keyColumn
                4,
                1000
        );
        NoopTransformerConfig originalTransformerConfig = new NoopTransformerConfig();
        JdbcLoaderConfig originalLoaderConfig = new JdbcLoaderConfig("old_target", 1000);

        String originalExtractorJson = objectMapper.writeValueAsString(originalExtractorConfig);
        String originalTransformerJson = objectMapper.writeValueAsString(originalTransformerConfig);
        String originalLoaderJson = objectMapper.writeValueAsString(originalLoaderConfig);

        LocalDateTime createdAt = LocalDateTime.of(2024, 1, 1, 10, 0);

        JobEntity existingEntity = JobEntity.builder()
                .id("test-job-5")
                .name("Original Name")
                .extractorType("sql")
                .extractorConfig(originalExtractorJson)
                .transformerType("noop")
                .transformerConfig(originalTransformerJson)
                .loaderType("sql")
                .loaderConfig(originalLoaderJson)
                .createdAt(createdAt)
                .updatedAt(createdAt)
                .createdBy("original_user")
                .status("ACTIVE")
                .description("Original description")
                .build();

        // Создаем обновленный EtlJob
        KafkaExtractorConfig updatedExtractorConfig = new KafkaExtractorConfig(
                "updated-topic",
                System.currentTimeMillis(),
                System.currentTimeMillis() + 1000,
                KafkaFormat.AVRO,
                16,
                2000
        );
        AvroToRecordTransformerConfig updatedTransformerConfig = new AvroToRecordTransformerConfig();
        FastSqlLoaderConfig updatedLoaderConfig = new FastSqlLoaderConfig("new_target");

        EtlJob updatedEtlJob = new EtlJob(
                "test-job-5",
                updatedExtractorConfig,
                updatedTransformerConfig,
                updatedLoaderConfig
        );

        // When
        JobEntity result = jobMapper.updateEntity(existingEntity, updatedEtlJob);

        // Then
        assertNotNull(result);
        assertEquals("test-job-5", result.getId());

        // Конфигурации должны обновиться
        assertEquals("kafka", result.getExtractorType());
        assertEquals("avro", result.getTransformerType());
        assertEquals("fast-sql", result.getLoaderType());
        assertTrue(result.getExtractorConfig().contains("updated-topic"));
        assertTrue(result.getLoaderConfig().contains("new_target"));

        // Метаданные НЕ должны измениться
        assertEquals("Original Name", result.getName());
        assertEquals(createdAt, result.getCreatedAt());
        assertEquals("original_user", result.getCreatedBy());
        assertEquals("ACTIVE", result.getStatus());
        assertEquals("Original description", result.getDescription());
    }

    @Test
    void roundTrip_shouldPreserveData() {
        // Given
        JdbcExtractorConfig extractorConfig = new JdbcExtractorConfig(
                Optional.of("SELECT id, name, value FROM source_table WHERE date > '2024-01-01'"),
                Optional.empty(), // table
                Optional.of("id"), // partitionColumn
                8,
                Optional.of("id"), // keyColumn
                8,
                50000
        );

        NoopTransformerConfig transformerConfig = new NoopTransformerConfig();

        FastSqlLoaderConfig loaderConfig = new FastSqlLoaderConfig("SUPPORT.dbo.target_table");

        EtlJob originalJob = new EtlJob(
                "roundtrip-job",
                extractorConfig,
                transformerConfig,
                loaderConfig
        );

        // When
        JobEntity entity = jobMapper.toEntity(originalJob, "test_user", "Round trip test");
        EtlJob convertedJob = jobMapper.toEtlJob(entity);

        // Then
        assertEquals(originalJob.jobId(), convertedJob.jobId());
        assertEquals(originalJob.extractorConfig().type(), convertedJob.extractorConfig().type());
        assertEquals(originalJob.transformerConfig().type(), convertedJob.transformerConfig().type());
        assertEquals(originalJob.loaderConfig().type(), convertedJob.loaderConfig().type());

        // Проверяем конкретные значения
        JdbcExtractorConfig originalExtractor = (JdbcExtractorConfig) originalJob.extractorConfig();
        JdbcExtractorConfig convertedExtractor = (JdbcExtractorConfig) convertedJob.extractorConfig();
        assertEquals(originalExtractor.sqlQuery(), convertedExtractor.sqlQuery());
        assertEquals(originalExtractor.threads(), convertedExtractor.threads());
        assertEquals(originalExtractor.streamBatchSize(), convertedExtractor.streamBatchSize());

        FastSqlLoaderConfig originalLoader = (FastSqlLoaderConfig) originalJob.loaderConfig();
        FastSqlLoaderConfig convertedLoader = (FastSqlLoaderConfig) convertedJob.loaderConfig();
        assertEquals(originalLoader.targetTable(), convertedLoader.targetTable());
    }

    @Test
    void toEntity_withKafkaToKafka_shouldHandleCorrectly() {
        // Given
        KafkaExtractorConfig extractorConfig = new KafkaExtractorConfig(
                "source-topic",
                System.currentTimeMillis(),
                System.currentTimeMillis() + 1000,
                KafkaFormat.AVRO,
                4,
                1000
        );

        RecordToAvroTransformerConfig transformerConfig = new RecordToAvroTransformerConfig("test-schema-subject");

        KafkaLoaderConfig loaderConfig = new KafkaLoaderConfig(
                "target-topic",
                KafkaFormat.AVRO
        );

        EtlJob etlJob = new EtlJob("kafka-job", extractorConfig, transformerConfig, loaderConfig);

        // When
        JobEntity entity = jobMapper.toEntity(etlJob);

        // Then
        assertNotNull(entity);
        assertEquals("kafka-job", entity.getId());
        assertEquals("kafka", entity.getExtractorType());
        assertEquals("record-to-avro", entity.getTransformerType());
        assertEquals("kafka", entity.getLoaderType());
        assertTrue(entity.getExtractorConfig().contains("source-topic"));
        assertTrue(entity.getLoaderConfig().contains("target-topic"));
    }

    @Test
    void toEntity_withComplexParameters_shouldSerializeCorrectly() {
        // Given
        KafkaExtractorConfig extractorConfig = new KafkaExtractorConfig(
                "test-topic",
                1704067200000L,
                1704153600000L,
                KafkaFormat.AVRO,
                4,
                1000
        );

        NoopTransformerConfig transformerConfig = new NoopTransformerConfig();

        JdbcLoaderConfig loaderConfig = new JdbcLoaderConfig("target_table", 1000);

        EtlJob etlJob = new EtlJob("complex-job", extractorConfig, transformerConfig, loaderConfig);

        // When
        JobEntity entity = jobMapper.toEntity(etlJob);
        EtlJob convertedJob = jobMapper.toEtlJob(entity);

        // Then
        assertTrue(convertedJob.extractorConfig() instanceof KafkaExtractorConfig);
        KafkaExtractorConfig kafkaConfig = (KafkaExtractorConfig) convertedJob.extractorConfig();
        assertEquals("test-topic", kafkaConfig.topic());
        assertEquals(1704067200000L, kafkaConfig.startTimestamp());
        assertEquals(1704153600000L, kafkaConfig.endTimestamp());
        assertEquals(KafkaFormat.AVRO, kafkaConfig.format());

        assertTrue(convertedJob.loaderConfig() instanceof JdbcLoaderConfig);
        JdbcLoaderConfig jdbcLoaderConfig = (JdbcLoaderConfig) convertedJob.loaderConfig();
        assertEquals("target_table", jdbcLoaderConfig.targetTable());
    }
}
