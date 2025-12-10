package ru.pospelov.etl.engine.api.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * JPA-сущность для хранения ETL job'ов в базе данных.
 * Представляет таблицу SUPPORT.service.tEtlJob с метаданными и type-safe конфигурациями компонентов.
 */
@Entity
@Table(name = "tEtlJob", schema = "service", catalog = "SUPPORT", indexes = {
    @Index(name = "IX_tEtlJob_ExtractorType", columnList = "ExtractorType"),
    @Index(name = "IX_tEtlJob_LoaderType", columnList = "LoaderType"),
    @Index(name = "IX_tEtlJob_Status", columnList = "Status"),
    @Index(name = "IX_tEtlJob_CreatedAt", columnList = "CreatedAt")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobEntity {

    /**
     * Уникальный идентификатор job'а
     */
    @Id
    @Column(name = "Id", nullable = false, length = 255)
    private String id;

    /**
     * Название job'а
     */
    @Column(name = "Name", length = 500)
    private String name;

    /**
     * Тип экстрактора: sql, kafka
     */
    @Column(name = "ExtractorType", nullable = false, length = 50)
    private String extractorType;

    /**
     * Конфигурация экстрактора в формате JSON
     */
    @Column(name = "ExtractorConfig", nullable = false, columnDefinition = "NVARCHAR(MAX)")
    private String extractorConfig;

    /**
     * Тип трансформера: noop, avro, record-to-avro
     */
    @Column(name = "TransformerType", nullable = false, length = 50)
    private String transformerType;

    /**
     * Конфигурация трансформера в формате JSON
     */
    @Column(name = "TransformerConfig", nullable = false, columnDefinition = "NVARCHAR(MAX)")
    private String transformerConfig;

    /**
     * Тип загрузчика: sql, fast-sql, kafka
     */
    @Column(name = "LoaderType", nullable = false, length = 50)
    private String loaderType;

    /**
     * Конфигурация загрузчика в формате JSON
     */
    @Column(name = "LoaderConfig", nullable = false, columnDefinition = "NVARCHAR(MAX)")
    private String loaderConfig;

    /**
     * Дата и время создания job'а
     */
    @Column(name = "CreatedAt", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /**
     * Дата и время последнего обновления job'а
     */
    @Column(name = "UpdatedAt", nullable = false)
    private LocalDateTime updatedAt;

    /**
     * Пользователь, создавший job
     */
    @Column(name = "CreatedBy", length = 255)
    private String createdBy;

    /**
     * Статус job'а: ACTIVE, DISABLED, ARCHIVED
     */
    @Column(name = "Status", nullable = false, length = 50)
    private String status;

    /**
     * Описание назначения job'а
     */
    @Column(name = "Description", columnDefinition = "NVARCHAR(MAX)")
    private String description;

    /**
     * Автоматически устанавливает createdAt и updatedAt перед первым сохранением
     */
    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        createdAt = now;
        updatedAt = now;
        if (status == null || status.trim().isEmpty()) {
            status = "ACTIVE";
        }
    }

    /**
     * Автоматически обновляет updatedAt перед каждым обновлением
     */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
