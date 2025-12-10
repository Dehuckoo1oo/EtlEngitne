package ru.pospelov.etl.engine.api.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.api.entity.JobEntity;
import ru.pospelov.etl.engine.config.extractor.*;
import ru.pospelov.etl.engine.config.transformer.*;
import ru.pospelov.etl.engine.config.loader.*;
import ru.pospelov.etl.engine.model.EtlJob;

/**
 * Mapper для конвертации между доменной моделью EtlJob и JPA-сущностью JobEntity.
 * Использует Jackson для сериализации/десериализации type-safe конфигураций в JSON.
 */
@Slf4j
@Component
public class JobMapper {

    private final ObjectMapper objectMapper;

    public JobMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper.copy();
        this.objectMapper.findAndRegisterModules();
    }

    /**
     * Конвертирует EtlJob в JobEntity для сохранения в БД
     *
     * @param etlJob доменная модель EtlJob
     * @return JPA-сущность JobEntity
     * @throws IllegalArgumentException если не удалось сериализовать конфигурации в JSON
     */
    public JobEntity toEntity(EtlJob etlJob) {
        return toEntity(etlJob, null, null);
    }

    /**
     * Конвертирует EtlJob в JobEntity с дополнительными метаданными
     *
     * @param etlJob      доменная модель EtlJob
     * @param createdBy   пользователь, создавший job
     * @param description описание job'а
     * @return JPA-сущность JobEntity
     * @throws IllegalArgumentException если не удалось сериализовать конфигурации в JSON
     */
    public JobEntity toEntity(EtlJob etlJob, String createdBy, String description) {
        try {
            // Сериализуем конфигурации в JSON
            String extractorConfigJson = objectMapper.writeValueAsString(etlJob.extractorConfig());
            String transformerConfigJson = objectMapper.writeValueAsString(etlJob.transformerConfig());
            String loaderConfigJson = objectMapper.writeValueAsString(etlJob.loaderConfig());

            return JobEntity.builder()
                    .id(etlJob.jobId())
                    .name(etlJob.jobId()) // По умолчанию name = id
                    .extractorType(etlJob.extractorConfig().type())
                    .extractorConfig(extractorConfigJson)
                    .transformerType(etlJob.transformerConfig().type())
                    .transformerConfig(transformerConfigJson)
                    .loaderType(etlJob.loaderConfig().type())
                    .loaderConfig(loaderConfigJson)
                    .createdBy(createdBy)
                    .description(description)
                    .status("ACTIVE")
                    .build();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize job configurations to JSON for job {}", etlJob.jobId(), e);
            throw new IllegalArgumentException("Failed to serialize job configurations: " + e.getMessage(), e);
        }
    }

    /**
     * Конвертирует JobEntity в EtlJob для использования в бизнес-логике
     *
     * @param entity JPA-сущность JobEntity
     * @return доменная модель EtlJob
     * @throws IllegalArgumentException если не удалось десериализовать конфигурации из JSON
     */
    public EtlJob toEtlJob(JobEntity entity) {
        try {
            // Десериализуем конфигурации из JSON на основе типов
            ExtractorConfig extractorConfig = deserializeExtractorConfig(
                    entity.getExtractorType(),
                    entity.getExtractorConfig()
            );

            TransformerConfig transformerConfig = deserializeTransformerConfig(
                    entity.getTransformerType(),
                    entity.getTransformerConfig()
            );

            LoaderConfig loaderConfig = deserializeLoaderConfig(
                    entity.getLoaderType(),
                    entity.getLoaderConfig()
            );

            return new EtlJob(
                    entity.getId(),
                    extractorConfig,
                    transformerConfig,
                    loaderConfig
            );
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize job configurations from JSON for job {}", entity.getId(), e);
            throw new IllegalArgumentException("Failed to deserialize job configurations: " + e.getMessage(), e);
        }
    }

    /**
     * Обновляет существующую JobEntity данными из EtlJob
     * Сохраняет метаданные (createdAt, createdBy) из существующей сущности
     *
     * @param existingEntity существующая JPA-сущность
     * @param etlJob         обновленная доменная модель
     * @return обновленная JPA-сущность
     * @throws IllegalArgumentException если не удалось сериализовать конфигурации в JSON
     */
    public JobEntity updateEntity(JobEntity existingEntity, EtlJob etlJob) {
        try {
            // Сериализуем конфигурации в JSON
            String extractorConfigJson = objectMapper.writeValueAsString(etlJob.extractorConfig());
            String transformerConfigJson = objectMapper.writeValueAsString(etlJob.transformerConfig());
            String loaderConfigJson = objectMapper.writeValueAsString(etlJob.loaderConfig());

            existingEntity.setExtractorType(etlJob.extractorConfig().type());
            existingEntity.setExtractorConfig(extractorConfigJson);
            existingEntity.setTransformerType(etlJob.transformerConfig().type());
            existingEntity.setTransformerConfig(transformerConfigJson);
            existingEntity.setLoaderType(etlJob.loaderConfig().type());
            existingEntity.setLoaderConfig(loaderConfigJson);
            // name, createdAt, createdBy, status, description остаются без изменений

            return existingEntity;
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize job configurations to JSON for job {}", etlJob.jobId(), e);
            throw new IllegalArgumentException("Failed to serialize job configurations: " + e.getMessage(), e);
        }
    }

    /**
     * Десериализует ExtractorConfig из JSON на основе типа
     */
    private ExtractorConfig deserializeExtractorConfig(String type, String json) throws JsonProcessingException {
        return switch (type) {
            case "sql" -> objectMapper.readValue(json, JdbcExtractorConfig.class);
            case "kafka" -> objectMapper.readValue(json, KafkaExtractorConfig.class);
            default -> throw new IllegalArgumentException("Unknown extractor type: " + type);
        };
    }

    /**
     * Десериализует TransformerConfig из JSON на основе типа
     */
    private TransformerConfig deserializeTransformerConfig(String type, String json) throws JsonProcessingException {
        return switch (type) {
            case "noop" -> objectMapper.readValue(json, NoopTransformerConfig.class);
            case "avro" -> objectMapper.readValue(json, AvroToRecordTransformerConfig.class);
            case "record-to-avro" -> objectMapper.readValue(json, RecordToAvroTransformerConfig.class);
            default -> throw new IllegalArgumentException("Unknown transformer type: " + type);
        };
    }

    /**
     * Десериализует LoaderConfig из JSON на основе типа
     */
    private LoaderConfig deserializeLoaderConfig(String type, String json) throws JsonProcessingException {
        return switch (type) {
            case "sql" -> objectMapper.readValue(json, JdbcLoaderConfig.class);
            case "fast-sql" -> objectMapper.readValue(json, FastSqlLoaderConfig.class);
            case "kafka" -> objectMapper.readValue(json, KafkaLoaderConfig.class);
            default -> throw new IllegalArgumentException("Unknown loader type: " + type);
        };
    }
}
