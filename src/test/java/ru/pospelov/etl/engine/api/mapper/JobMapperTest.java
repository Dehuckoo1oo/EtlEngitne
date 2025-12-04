package ru.pospelov.etl.engine.api.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.pospelov.etl.engine.api.entity.JobEntity;
import ru.pospelov.etl.engine.model.EtlJob;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit-тесты для JobMapper
 */
class JobMapperTest {

    private JobMapper jobMapper;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        jobMapper = new JobMapper(objectMapper);
    }

    @Test
    void toEntity_shouldConvertEtlJobToJobEntity() {
        // Given
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("loaderType", "kafka");
        params.put("threads", 4);

        EtlJob etlJob = new EtlJob(
                "test-job-1",
                "SELECT * FROM test_table",
                "target_table",
                params
        );

        // When
        JobEntity entity = jobMapper.toEntity(etlJob);

        // Then
        assertNotNull(entity);
        assertEquals("test-job-1", entity.getId());
        assertEquals("test-job-1", entity.getName()); // По умолчанию name = id
        assertEquals("SELECT * FROM test_table", entity.getSourceQuery());
        assertEquals("target_table", entity.getTarget());
        assertEquals("ACTIVE", entity.getStatus());
        assertNotNull(entity.getParams());
        assertTrue(entity.getParams().contains("extractorType"));
        assertTrue(entity.getParams().contains("sql"));
    }

    @Test
    void toEntity_withMetadata_shouldIncludeCreatedByAndDescription() {
        // Given
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "kafka");

        EtlJob etlJob = new EtlJob("test-job-2", null, null, params);

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
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("loaderType", "kafka");
        params.put("threads", 8);

        String paramsJson = objectMapper.writeValueAsString(params);

        JobEntity entity = JobEntity.builder()
                .id("test-job-3")
                .name("Test Job 3")
                .sourceQuery("SELECT * FROM orders")
                .target("kafka-topic")
                .params(paramsJson)
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
        assertEquals("test-job-3", etlJob.getJobId());
        assertEquals("SELECT * FROM orders", etlJob.getSourceQuery());
        assertEquals("kafka-topic", etlJob.getTargetTable());
        assertNotNull(etlJob.getParameters());
        assertEquals("sql", etlJob.getParam("extractorType"));
        assertEquals("kafka", etlJob.getParam("loaderType"));
        assertEquals(8, etlJob.getParam("threads"));
    }

    @Test
    void toEtlJob_withInvalidJson_shouldThrowException() {
        // Given
        JobEntity entity = JobEntity.builder()
                .id("test-job-4")
                .params("invalid json {{{")
                .build();

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> jobMapper.toEtlJob(entity));
    }

    @Test
    void updateEntity_shouldUpdateOnlyRelevantFields() throws Exception {
        // Given
        Map<String, Object> originalParams = new HashMap<>();
        originalParams.put("extractorType", "sql");

        String originalParamsJson = objectMapper.writeValueAsString(originalParams);

        LocalDateTime createdAt = LocalDateTime.of(2024, 1, 1, 10, 0);

        JobEntity existingEntity = JobEntity.builder()
                .id("test-job-5")
                .name("Original Name")
                .sourceQuery("SELECT * FROM old_table")
                .target("old_target")
                .params(originalParamsJson)
                .createdAt(createdAt)
                .updatedAt(createdAt)
                .createdBy("original_user")
                .status("ACTIVE")
                .description("Original description")
                .build();

        Map<String, Object> updatedParams = new HashMap<>();
        updatedParams.put("extractorType", "kafka");
        updatedParams.put("threads", 16);

        EtlJob updatedEtlJob = new EtlJob(
                "test-job-5",
                "SELECT * FROM new_table",
                "new_target",
                updatedParams
        );

        // When
        JobEntity result = jobMapper.updateEntity(existingEntity, updatedEtlJob);

        // Then
        assertNotNull(result);
        assertEquals("test-job-5", result.getId());
        assertEquals("SELECT * FROM new_table", result.getSourceQuery());
        assertEquals("new_target", result.getTarget());

        // Метаданные НЕ должны измениться
        assertEquals("Original Name", result.getName());
        assertEquals(createdAt, result.getCreatedAt());
        assertEquals("original_user", result.getCreatedBy());
        assertEquals("ACTIVE", result.getStatus());
        assertEquals("Original description", result.getDescription());

        // Параметры должны обновиться
        assertTrue(result.getParams().contains("kafka"));
        assertTrue(result.getParams().contains("16"));
    }

    @Test
    void roundTrip_shouldPreserveData() {
        // Given
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("transformerType", "noop");
        params.put("loaderType", "fast-sql");
        params.put("threads", 8);
        params.put("streamBatchSize", 50000);

        EtlJob originalJob = new EtlJob(
                "roundtrip-job",
                "SELECT id, name, value FROM source_table WHERE date > '2024-01-01'",
                "SUPPORT.dbo.target_table",
                params
        );

        // When
        JobEntity entity = jobMapper.toEntity(originalJob, "test_user", "Round trip test");
        EtlJob convertedJob = jobMapper.toEtlJob(entity);

        // Then
        assertEquals(originalJob.getJobId(), convertedJob.getJobId());
        assertEquals(originalJob.getSourceQuery(), convertedJob.getSourceQuery());
        assertEquals(originalJob.getTargetTable(), convertedJob.getTargetTable());
        assertEquals(originalJob.getParam("extractorType"), convertedJob.getParam("extractorType"));
        assertEquals(originalJob.getParam("transformerType"), convertedJob.getParam("transformerType"));
        assertEquals(originalJob.getParam("loaderType"), convertedJob.getParam("loaderType"));
        assertEquals(originalJob.getParam("threads"), convertedJob.getParam("threads"));
        assertEquals(originalJob.getParam("streamBatchSize"), convertedJob.getParam("streamBatchSize"));
    }

    @Test
    void toEntity_withNullSourceQueryAndTarget_shouldHandleCorrectly() {
        // Given
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "kafka");
        params.put("loaderType", "kafka");

        EtlJob etlJob = new EtlJob("kafka-job", null, null, params);

        // When
        JobEntity entity = jobMapper.toEntity(etlJob);

        // Then
        assertNotNull(entity);
        assertEquals("kafka-job", entity.getId());
        assertNull(entity.getSourceQuery());
        assertNull(entity.getTarget());
        assertNotNull(entity.getParams());
    }

    @Test
    void toEntity_withComplexParameters_shouldSerializeCorrectly() {
        // Given
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "kafka");
        params.put("topic", "test-topic");
        params.put("startTimestamp", 1704067200000L);
        params.put("endTimestamp", 1704153600000L);
        params.put("partitions", 72);

        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put("compression.type", "snappy");
        additionalConfig.put("batch.size", "16384");
        params.put("kafkaConfig", additionalConfig);

        EtlJob etlJob = new EtlJob("complex-job", null, null, params);

        // When
        JobEntity entity = jobMapper.toEntity(etlJob);
        EtlJob convertedJob = jobMapper.toEtlJob(entity);

        // Then
        assertEquals("test-topic", convertedJob.getParam("topic"));
        assertEquals(1704067200000L, convertedJob.getParam("startTimestamp"));
        assertEquals(72, convertedJob.getParam("partitions"));

        @SuppressWarnings("unchecked")
        Map<String, String> convertedConfig = (Map<String, String>) convertedJob.getParam("kafkaConfig");
        assertNotNull(convertedConfig);
        assertEquals("snappy", convertedConfig.get("compression.type"));
        assertEquals("16384", convertedConfig.get("batch.size"));
    }
}
