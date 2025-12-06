package ru.pospelov.etl.engine.api.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.api.entity.JobEntity;
import ru.pospelov.etl.engine.model.EtlJob;

import java.util.Map;

/**
 * Mapper для конвертации между доменной моделью EtlJob и JPA-сущностью JobEntity.
 * Использует Jackson для сериализации/десериализации параметров в JSON.
 */
@Slf4j
@Component
public class JobMapper {

    private final ObjectMapper objectMapper;

    public JobMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Конвертирует EtlJob в JobEntity для сохранения в БД
     *
     * @param etlJob доменная модель EtlJob
     * @return JPA-сущность JobEntity
     * @throws IllegalArgumentException если не удалось сериализовать параметры в JSON
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
     * @throws IllegalArgumentException если не удалось сериализовать параметры в JSON
     */
    public JobEntity toEntity(EtlJob etlJob, String createdBy, String description) {
        try {
            String paramsJson = objectMapper.writeValueAsString(etlJob.getParameters());

            return JobEntity.builder()
                    .id(etlJob.getJobId())
                    .name(etlJob.getJobId()) // По умолчанию name = id
                    .source(etlJob.getSource())
                    .target(etlJob.getTargetTable())
                    .params(paramsJson)
                    .createdBy(createdBy)
                    .description(description)
                    .status("ACTIVE")
                    .build();
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize job parameters to JSON for job {}", etlJob.getJobId(), e);
            throw new IllegalArgumentException("Failed to serialize job parameters: " + e.getMessage(), e);
        }
    }

    /**
     * Конвертирует JobEntity в EtlJob для использования в бизнес-логике
     *
     * @param entity JPA-сущность JobEntity
     * @return доменная модель EtlJob
     * @throws IllegalArgumentException если не удалось десериализовать параметры из JSON
     */
    public EtlJob toEtlJob(JobEntity entity) {
        try {
            Map<String, Object> parameters = objectMapper.readValue(
                    entity.getParams(),
                    new TypeReference<Map<String, Object>>() {
                    }
            );

            return new EtlJob(
                    entity.getId(),
                    entity.getSource(),
                    entity.getTarget(),
                    parameters
            );
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize job parameters from JSON for job {}", entity.getId(), e);
            throw new IllegalArgumentException("Failed to deserialize job parameters: " + e.getMessage(), e);
        }
    }

    /**
     * Обновляет существующую JobEntity данными из EtlJob
     * Сохраняет метаданные (createdAt, createdBy) из существующей сущности
     *
     * @param existingEntity существующая JPA-сущность
     * @param etlJob         обновленная доменная модель
     * @return обновленная JPA-сущность
     * @throws IllegalArgumentException если не удалось сериализовать параметры в JSON
     */
    public JobEntity updateEntity(JobEntity existingEntity, EtlJob etlJob) {
        try {
            String paramsJson = objectMapper.writeValueAsString(etlJob.getParameters());

            existingEntity.setSource(etlJob.getSource());
            existingEntity.setTarget(etlJob.getTargetTable());
            existingEntity.setParams(paramsJson);
            // name, createdAt, createdBy, status, description остаются без изменений

            return existingEntity;
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize job parameters to JSON for job {}", etlJob.getJobId(), e);
            throw new IllegalArgumentException("Failed to serialize job parameters: " + e.getMessage(), e);
        }
    }
}
