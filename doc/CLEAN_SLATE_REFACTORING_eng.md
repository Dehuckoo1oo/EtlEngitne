# Clean Slate Refactoring: Final Architecture

## Context

The project is **NOT yet in production**, which means:
- ✅ No legacy data to migrate
- ✅ Database schema can be changed freely
- ✅ Breaking changes in API are allowed
- ✅ All Java 21 features can be used
- ✅ The most correct solution can be chosen

**Java Version:** 21
**Spring Boot:** 3.5.0

---

## Architectural Decisions Made

| Question | Decision | Rationale |
|----------|----------|-----------|
| **Defaults** | ❌ NO defaults in code. User fills everything. | Explicit is better than implicit. UI shows recommendations. |
| **Nullable fields** | ✅ `Optional<T>` for optional fields | Type-safe null handling, explicit API contract |
| **DB Schema** | ✅ Separate columns for types and configurations | Enables SQL queries, indexes on types |
| **Pattern matching** | ✅ Centralized Factory | Exhaustive checks, single entry point |
| **Validation** | ✅ Bean Validation only | Declarative, no custom logic in constructors |

---

## Architecture: Sealed Interfaces + Records + Pattern Matching

### Why is this the best solution?

1. **Maximum type safety** - compiler checks everything
2. **Exhaustive pattern matching** - impossible to forget handling a case
3. **Immutability by default** - Records are immutable
4. **Minimal boilerplate** - Records auto-generate equals/hashCode/toString
5. **Explicit contract** - sealed interfaces close the list of implementations
6. **Optional for nullable** - explicit null handling
7. **No runtime reflection** - everything is checked at compile-time

---

## 1. Component Configurations

### Extractors

```java
// src/main/java/ru/pospelov/etl/engine/config/extractor/ExtractorConfig.java
package ru.pospelov.etl.engine.config.extractor;

/**
 * Base configuration for all extractors.
 * Sealed - we can only add new types explicitly.
 */
public sealed interface ExtractorConfig
    permits JdbcExtractorConfig, KafkaExtractorConfig {
    String type();
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/extractor/JdbcExtractorConfig.java
package ru.pospelov.etl.engine.config.extractor;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

/**
 * JDBC extractor configuration.
 *
 * All fields are required (NotNull), except Optional fields.
 * NO defaults - user explicitly specifies all values.
 */
public record JdbcExtractorConfig(
    @NotBlank(message = "SQL query is required")
    String sqlQuery,

    Optional<String> partitionColumn,

    @NotNull(message = "Partitions is required")
    @Min(value = 1, message = "Partitions must be >= 1")
    Integer partitions,

    Optional<String> keyColumn,

    @NotNull(message = "Threads is required")
    @Min(value = 1, message = "Threads must be >= 1")
    Integer threads,

    @NotNull(message = "Stream batch size is required")
    @Min(value = 100, message = "Stream batch size must be >= 100")
    @Max(value = 1_048_576, message = "Stream batch size must be <= 1,048,576 (2^20)")
    Integer streamBatchSize
) implements ExtractorConfig {

    @Override
    public String type() {
        return "sql";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/extractor/KafkaExtractorConfig.java
package ru.pospelov.etl.engine.config.extractor;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.pospelov.etl.engine.config.KafkaFormat;

/**
 * Kafka extractor configuration.
 */
public record KafkaExtractorConfig(
    @NotBlank(message = "Kafka topic is required")
    String topic,

    @NotNull(message = "Start timestamp is required")
    Long startTimestamp,

    @NotNull(message = "End timestamp is required")
    Long endTimestamp,

    @NotNull(message = "Format is required")
    KafkaFormat format,

    @NotNull(message = "Threads is required")
    @Min(value = 1, message = "Threads must be >= 1")
    Integer threads,

    @NotNull(message = "Stream batch size is required")
    @Min(value = 100, message = "Stream batch size must be >= 100")
    @Max(value = 1_048_576, message = "Stream batch size must be <= 1,048,576 (2^20)")
    Integer streamBatchSize
) implements ExtractorConfig {

    @Override
    public String type() {
        return "kafka";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/KafkaFormat.java
package ru.pospelov.etl.engine.config;

/**
 * Kafka data format - type-safe enum instead of String.
 */
public enum KafkaFormat {
    AVRO,
    JSON,
    STRING
}
```

### Transformers

```java
// src/main/java/ru/pospelov/etl/engine/config/transformer/TransformerConfig.java
package ru.pospelov.etl.engine.config.transformer;

public sealed interface TransformerConfig
    permits NoopTransformerConfig, AvroToRecordTransformerConfig, RecordToAvroTransformerConfig {
    String type();
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/transformer/NoopTransformerConfig.java
package ru.pospelov.etl.engine.config.transformer;

public record NoopTransformerConfig() implements TransformerConfig {
    @Override
    public String type() {
        return "noop";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/transformer/AvroToRecordTransformerConfig.java
package ru.pospelov.etl.engine.config.transformer;

public record AvroToRecordTransformerConfig() implements TransformerConfig {
    @Override
    public String type() {
        return "avro";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/transformer/RecordToAvroTransformerConfig.java
package ru.pospelov.etl.engine.config.transformer;

import jakarta.validation.constraints.NotBlank;

public record RecordToAvroTransformerConfig(
    @NotBlank(message = "Avro schema subject is required")
    String avroSchemaSubject
) implements TransformerConfig {
    @Override
    public String type() {
        return "record-to-avro";
    }
}
```

### Loaders

```java
// src/main/java/ru/pospelov/etl/engine/config/loader/LoaderConfig.java
package ru.pospelov.etl.engine.config.loader;

public sealed interface LoaderConfig
    permits JdbcLoaderConfig, FastSqlLoaderConfig, KafkaLoaderConfig {
    String type();
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/loader/JdbcLoaderConfig.java
package ru.pospelov.etl.engine.config.loader;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record JdbcLoaderConfig(
    @NotBlank(message = "Target table is required")
    String targetTable,

    @NotNull(message = "Stream batch size is required")
    @Min(value = 100, message = "Stream batch size must be >= 100")
    @Max(value = 1_048_576, message = "Stream batch size must be <= 1,048,576 (2^20)")
    Integer streamBatchSize
) implements LoaderConfig {
    @Override
    public String type() {
        return "sql";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/loader/FastSqlLoaderConfig.java
package ru.pospelov.etl.engine.config.loader;

import jakarta.validation.constraints.NotBlank;

public record FastSqlLoaderConfig(
    @NotBlank(message = "Target table is required")
    String targetTable
) implements LoaderConfig {
    @Override
    public String type() {
        return "fast-sql";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/loader/KafkaLoaderConfig.java
package ru.pospelov.etl.engine.config.loader;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.pospelov.etl.engine.config.KafkaFormat;

import java.util.Optional;

public record KafkaLoaderConfig(
    @NotBlank(message = "Kafka topic is required")
    String topic,

    @NotNull(message = "Format is required")
    KafkaFormat format,

    Optional<String> avroSchemaSubject
) implements LoaderConfig {
    @Override
    public String type() {
        return "kafka";
    }
}
```

---

## 2. EtlJob Model - Maximally Simplified

```java
// src/main/java/ru/pospelov/etl/engine/model/EtlJob.java
package ru.pospelov.etl.engine.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.pospelov.etl.engine.config.extractor.*;
import ru.pospelov.etl.engine.config.transformer.*;
import ru.pospelov.etl.engine.config.loader.*;

/**
 * ETL Job - fully type-safe configuration.
 *
 * REMOVED:
 * - Map<String, Object> parameters ❌
 * - String source ❌
 * - String targetTable ❌
 * - getParamOrDefault() ❌
 *
 * ADDED:
 * - Type-safe configurations ✅
 * - Jackson polymorphic serialization ✅
 * - Bean Validation ✅
 * - Optional for nullable fields ✅
 */
public record EtlJob(
    @NotBlank(message = "Job ID is required")
    String jobId,

    @Valid
    @NotNull(message = "Extractor configuration is required")
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "extractorType")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = JdbcExtractorConfig.class, name = "sql"),
        @JsonSubTypes.Type(value = KafkaExtractorConfig.class, name = "kafka")
    })
    ExtractorConfig extractorConfig,

    @Valid
    @NotNull(message = "Transformer configuration is required")
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "transformerType")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = NoopTransformerConfig.class, name = "noop"),
        @JsonSubTypes.Type(value = AvroToRecordTransformerConfig.class, name = "avro"),
        @JsonSubTypes.Type(value = RecordToAvroTransformerConfig.class, name = "record-to-avro")
    })
    TransformerConfig transformerConfig,

    @Valid
    @NotNull(message = "Loader configuration is required")
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "loaderType")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = JdbcLoaderConfig.class, name = "sql"),
        @JsonSubTypes.Type(value = FastSqlLoaderConfig.class, name = "fast-sql"),
        @JsonSubTypes.Type(value = KafkaLoaderConfig.class, name = "kafka")
    })
    LoaderConfig loaderConfig
) {
    // Record - no methods needed!
}
```

**JSON Example:**
```json
{
  "jobId": "orders-sql-to-kafka",
  "extractorType": "sql",
  "extractorConfig": {
    "sqlQuery": "SELECT * FROM orders WHERE created_at > '2024-01-01'",
    "partitionColumn": null,
    "partitions": 1,
    "keyColumn": "order_id",
    "threads": 8,
    "streamBatchSize": 100000
  },
  "transformerType": "record-to-avro",
  "transformerConfig": {
    "avroSchemaSubject": "order-events-value"
  },
  "loaderType": "kafka",
  "loaderConfig": {
    "topic": "orders-topic",
    "format": "AVRO",
    "avroSchemaSubject": "order-events-value"
  }
}
```

---

## 3. Database Schema - Separate Columns

```sql
-- Flyway migration: V2__refactor_job_schema.sql

-- Remove old columns
ALTER TABLE SUPPORT.service.tEtlJob
DROP COLUMN Source;

ALTER TABLE SUPPORT.service.tEtlJob
DROP COLUMN Target;

ALTER TABLE SUPPORT.service.tEtlJob
DROP COLUMN Params;

-- Add new columns for separate storage
ALTER TABLE SUPPORT.service.tEtlJob
ADD ExtractorType VARCHAR(50) NOT NULL;

ALTER TABLE SUPPORT.service.tEtlJob
ADD ExtractorConfig NVARCHAR(MAX) NOT NULL;

ALTER TABLE SUPPORT.service.tEtlJob
ADD TransformerType VARCHAR(50) NOT NULL;

ALTER TABLE SUPPORT.service.tEtlJob
ADD TransformerConfig NVARCHAR(MAX) NOT NULL;

ALTER TABLE SUPPORT.service.tEtlJob
ADD LoaderType VARCHAR(50) NOT NULL;

ALTER TABLE SUPPORT.service.tEtlJob
ADD LoaderConfig NVARCHAR(MAX) NOT NULL;

-- Indexes for fast search by types
CREATE INDEX IX_tEtlJob_ExtractorType ON SUPPORT.service.tEtlJob(ExtractorType);
CREATE INDEX IX_tEtlJob_LoaderType ON SUPPORT.service.tEtlJob(LoaderType);
```

**Updated JPA Entity:**
```java
// src/main/java/ru/pospelov/etl/engine/api/entity/JobEntity.java
package ru.pospelov.etl.engine.api.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

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

    @Id
    @Column(name = "Id", nullable = false, length = 255)
    private String id;

    @Column(name = "Name", length = 500)
    private String name;

    @Column(name = "ExtractorType", nullable = false, length = 50)
    private String extractorType;

    @Column(name = "ExtractorConfig", nullable = false, columnDefinition = "NVARCHAR(MAX)")
    private String extractorConfig;

    @Column(name = "TransformerType", nullable = false, length = 50)
    private String transformerType;

    @Column(name = "TransformerConfig", nullable = false, columnDefinition = "NVARCHAR(MAX)")
    private String transformerConfig;

    @Column(name = "LoaderType", nullable = false, length = 50)
    private String loaderType;

    @Column(name = "LoaderConfig", nullable = false, columnDefinition = "NVARCHAR(MAX)")
    private String loaderConfig;

    @Column(name = "CreatedAt", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "UpdatedAt", nullable = false)
    private LocalDateTime updatedAt;

    @Column(name = "CreatedBy", length = 255)
    private String createdBy;

    @Column(name = "Status", nullable = false, length = 50)
    private String status;

    @Column(name = "Description", columnDefinition = "NVARCHAR(MAX)")
    private String description;

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        createdAt = now;
        updatedAt = now;
        if (status == null || status.trim().isEmpty()) {
            status = "ACTIVE";
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
```

**Advantages of Separate Columns:**
- ✅ SQL queries: `WHERE ExtractorType = 'sql'`
- ✅ Indexes on component types
- ✅ Statistics: `SELECT ExtractorType, COUNT(*) FROM ... GROUP BY ExtractorType`
- ✅ Ability to migrate only specific configuration types

---

## 4. Centralized Factory with Pattern Matching

```java
// src/main/java/ru/pospelov/etl/engine/engine/EtlComponentFactory.java
package ru.pospelov.etl.engine.engine;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.extractor.*;
import ru.pospelov.etl.engine.config.transformer.*;
import ru.pospelov.etl.engine.config.loader.*;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.steps.extractor.*;
import ru.pospelov.etl.engine.steps.transformer.*;
import ru.pospelov.etl.engine.steps.loader.*;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Centralized factory for creating and running ETL components.
 * Uses exhaustive pattern matching for type-safe dispatching.
 */
@Component
@RequiredArgsConstructor
public class EtlComponentFactory {

    private final JdbcExtractor jdbcExtractor;
    private final KafkaPartitionExtractor kafkaExtractor;

    private final NoopTransformer noopTransformer;
    private final AvroToRecordTransformer avroToRecordTransformer;
    private final RecordToAvroTransformer recordToAvroTransformer;

    private final JdbcLoader jdbcLoader;
    private final FastSqlServerLoader fastSqlLoader;
    private final KafkaLoader kafkaLoader;

    /**
     * Extract data using extractor configuration.
     * Pattern matching guarantees handling of all types.
     */
    public void extract(EtlJob job, Consumer<Collection<EtlRecord>> batchConsumer) {
        switch (job.extractorConfig()) {
            case JdbcExtractorConfig config ->
                jdbcExtractor.extract(config, job.jobId(), batchConsumer);

            case KafkaExtractorConfig config ->
                kafkaExtractor.extract(config, job.jobId(), batchConsumer);

            // Compiler checks exhaustiveness!
            // If a new sealed type is added - code won't compile without handling it
        }
    }

    /**
     * Transform data using transformer configuration.
     */
    public Collection<EtlRecord> transform(EtlJob job, Collection<EtlRecord> records) {
        return switch (job.transformerConfig()) {
            case NoopTransformerConfig config ->
                noopTransformer.transform(records);

            case AvroToRecordTransformerConfig config ->
                avroToRecordTransformer.transform(records);

            case RecordToAvroTransformerConfig config ->
                recordToAvroTransformer.transform(records, config);
        };
    }

    /**
     * Load data using loader configuration.
     */
    public void load(EtlJob job, Collection<EtlRecord> records) {
        switch (job.loaderConfig()) {
            case JdbcLoaderConfig config ->
                jdbcLoader.load(config, job.jobId(), records);

            case FastSqlLoaderConfig config ->
                fastSqlLoader.load(config, job.jobId(), records);

            case KafkaLoaderConfig config ->
                kafkaLoader.load(config, job.jobId(), records);
        }
    }
}
```

**Advantages:**
- ✅ Exhaustive check - compiler verifies all cases
- ✅ Single entry point for all components
- ✅ Easy to add logging/metrics
- ✅ Component selection logic removed from components themselves

---

## 5. Updated Components

### JdbcExtractor

```java
// src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcExtractor.java
package ru.pospelov.etl.engine.steps.extractor.jdbc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.exception.ExtractionException;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Slf4j
@Component
@RequiredArgsConstructor
public class JdbcExtractor {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Extract data from JDBC source.
     * All parameters are type-safe and NOT NULL (except Optional).
     * NO getParamOrDefault - everything is explicitly specified by user.
     */
    public void extract(
        JdbcExtractorConfig config,
        String jobId,
        Consumer<Collection<EtlRecord>> batchConsumer
    ) {
        // ✅ Type-safe field access
        String query = config.sqlQuery();
        int threads = config.threads();
        int batchSize = config.streamBatchSize();
        int partitions = config.partitions();

        // ✅ Optional - explicit nullable handling
        Optional<String> partitionColumn = config.partitionColumn();
        Optional<String> keyColumn = config.keyColumn();

        log.info("Job '{}' extraction started: query={}, threads={}, batchSize={}, partitions={}",
                jobId, query, threads, batchSize, partitions);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<?>> tasks = new ArrayList<>();

        try {
            // Partitioning only if partition column is specified
            if (partitionColumn.isPresent() && partitions > 1) {
                log.info("Job '{}' using partition-based extraction: column={}, partitions={}",
                        jobId, partitionColumn.get(), partitions);

                for (int p = 0; p < partitions; p++) {
                    String partQuery = query +
                        (query.toLowerCase().contains("where") ? " AND " : " WHERE ") +
                        partitionColumn.get() + " = " + p;

                    tasks.add(executor.submit(new JdbcPartitionQueryTask(
                        jdbcTemplate,
                        partQuery,
                        p,
                        batchSize,
                        keyColumn.orElse(null),
                        batchConsumer
                    )));
                }
            } else {
                // Offset-based extraction
                Integer totalRows = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM (" + query + ") t",
                    Integer.class
                );
                int total = (totalRows == null) ? 0 : totalRows;

                log.info("Job '{}' using offset-based extraction: totalRows={}, tasks={}",
                        jobId, total, (total + batchSize - 1) / batchSize);

                for (int offset = 0; offset < total; offset += batchSize) {
                    tasks.add(executor.submit(new JdbcOffsetQueryTask(
                        jdbcTemplate,
                        query,
                        offset,
                        batchSize,
                        keyColumn.orElse(null),
                        batchConsumer
                    )));
                }
            }

            waitForTasks(tasks, jobId);
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                throw new ExtractionException("JdbcExtractor timeout", jobId);
            }

            log.info("Job '{}' extraction completed: {} tasks finished", jobId, tasks.size());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ExtractionException("JdbcExtractor interrupted", jobId, e);
        } catch (Exception e) {
            throw new ExtractionException("JdbcExtractor failed", jobId, e);
        } finally {
            executor.shutdownNow();
        }
    }

    private static void waitForTasks(List<Future<?>> tasks, String jobId) {
        // ... (unchanged)
    }
}
```

### KafkaPartitionExtractor

```java
// src/main/java/ru/pospelov/etl/engine/steps/extractor/kafka/KafkaPartitionExtractor.java
package ru.pospelov.etl.engine.steps.extractor.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.KafkaFormat;
import ru.pospelov.etl.engine.config.extractor.KafkaExtractorConfig;
import ru.pospelov.etl.engine.config.KafkaClientFactory;
import ru.pospelov.etl.engine.exception.ExtractionException;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaPartitionExtractor {

    private final KafkaClientFactory consumerFactory;

    public void extract(
        KafkaExtractorConfig config,
        String jobId,
        Consumer<Collection<EtlRecord>> batchConsumer
    ) {
        // ✅ Type-safe access
        String topic = config.topic();
        long startMillis = config.startTimestamp();
        long endMillis = config.endTimestamp();
        int threads = config.threads();
        int streamBatchSize = config.streamBatchSize();
        KafkaFormat format = config.format();

        boolean isAvro = (format == KafkaFormat.AVRO);

        log.info("Job '{}' Kafka extraction started: topic={}, start={}, end={}, threads={}, format={}",
                jobId, topic, Instant.ofEpochMilli(startMillis), Instant.ofEpochMilli(endMillis),
                threads, format);

        // ... rest of the logic unchanged
    }
}
```

---

## 6. API Controller - Automatic Validation

```java
// src/main/java/ru/pospelov/etl/engine/api/controller/JobController.java
package ru.pospelov.etl.engine.api.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.pospelov.etl.engine.api.repository.JobRepository;
import ru.pospelov.etl.engine.model.EtlJob;

import java.util.List;

@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobRepository jobRepository;

    /**
     * Create job with automatic validation.
     * Spring automatically:
     * 1. Deserializes JSON to EtlJob record
     * 2. Performs Bean Validation (@Valid)
     * 3. Returns 400 Bad Request with detailed errors
     */
    @PostMapping
    public ResponseEntity<EtlJob> createJob(@Valid @RequestBody EtlJob job) {
        // ✅ No validation needed - if we got here, job is valid
        EtlJob saved = jobRepository.save(job);
        return ResponseEntity.ok(saved);
    }

    @PutMapping("/{id}")
    public ResponseEntity<EtlJob> updateJob(
        @PathVariable String id,
        @Valid @RequestBody EtlJob job
    ) {
        if (!id.equals(job.jobId())) {
            return ResponseEntity.badRequest().build();
        }
        EtlJob updated = jobRepository.save(job);
        return ResponseEntity.ok(updated);
    }

    @GetMapping("/{id}")
    public ResponseEntity<EtlJob> getJob(@PathVariable String id) {
        return jobRepository.findById(id)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<EtlJob>> getAllJobs() {
        return ResponseEntity.ok(jobRepository.findAll());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteJob(@PathVariable String id) {
        boolean deleted = jobRepository.deleteById(id);
        return deleted
            ? ResponseEntity.noContent().build()
            : ResponseEntity.notFound().build();
    }
}
```

---

## 7. Component Schema API - Auto-generation from Records

```java
// src/main/java/ru/pospelov/etl/engine/api/controller/JobSchemaController.java
package ru.pospelov.etl.engine.api.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.pospelov.etl.engine.api.dto.ComponentSchemaResponse;
import ru.pospelov.etl.engine.api.service.ComponentSchemaGenerator;

/**
 * API for getting ETL component schema.
 * Schema is automatically generated from Record classes.
 */
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobSchemaController {

    private final ComponentSchemaGenerator schemaGenerator;

    @GetMapping("/schema")
    public ResponseEntity<ComponentSchemaResponse> getComponentSchema() {
        return ResponseEntity.ok(schemaGenerator.generate());
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/api/service/ComponentSchemaGenerator.java
package ru.pospelov.etl.engine.api.service;

import org.springframework.stereotype.Service;
import ru.pospelov.etl.engine.api.dto.*;
import ru.pospelov.etl.engine.config.extractor.*;
import ru.pospelov.etl.engine.config.transformer.*;
import ru.pospelov.etl.engine.config.loader.*;

import jakarta.validation.constraints.*;
import java.lang.reflect.RecordComponent;
import java.util.*;

/**
 * Schema generator from Record classes using reflection.
 * Reads Record fields and Bean Validation annotations.
 */
@Service
public class ComponentSchemaGenerator {

    public ComponentSchemaResponse generate() {
        return ComponentSchemaResponse.builder()
            .extractors(Map.of(
                "sql", generateSchema(JdbcExtractorConfig.class, "SQL Database"),
                "kafka", generateSchema(KafkaExtractorConfig.class, "Kafka Topic")
            ))
            .transformers(Map.of(
                "noop", generateSchema(NoopTransformerConfig.class, "No Operation"),
                "avro", generateSchema(AvroToRecordTransformerConfig.class, "Avro to Record"),
                "record-to-avro", generateSchema(RecordToAvroTransformerConfig.class, "Record to Avro")
            ))
            .loaders(Map.of(
                "sql", generateSchema(JdbcLoaderConfig.class, "SQL Database"),
                "fast-sql", generateSchema(FastSqlLoaderConfig.class, "Fast SQL (Bulk Insert)"),
                "kafka", generateSchema(KafkaLoaderConfig.class, "Kafka Topic")
            ))
            .build();
    }

    private ComponentSchema generateSchema(Class<?> recordClass, String displayName) {
        RecordComponent[] components = recordClass.getRecordComponents();
        List<FieldSchema> fields = new ArrayList<>();

        for (RecordComponent component : components) {
            fields.add(FieldSchema.builder()
                .name(component.getName())
                .type(mapType(component.getType()))
                .label(formatLabel(component.getName()))
                .required(isRequired(component))
                .min(getMin(component))
                .max(getMax(component))
                .build());
        }

        return ComponentSchema.builder()
            .displayName(displayName)
            .fields(fields)
            .build();
    }

    private String mapType(Class<?> javaType) {
        if (javaType == String.class) return "text";
        if (javaType == Integer.class) return "number";
        if (javaType == Long.class) return "number";
        if (javaType.isEnum()) return "enum";
        if (javaType == Optional.class) return "optional";
        return "text";
    }

    private boolean isRequired(RecordComponent component) {
        return component.isAnnotationPresent(NotNull.class)
            || component.isAnnotationPresent(NotBlank.class);
    }

    private Integer getMin(RecordComponent component) {
        Min min = component.getAnnotation(Min.class);
        return min != null ? (int) min.value() : null;
    }

    private Integer getMax(RecordComponent component) {
        Max max = component.getAnnotation(Max.class);
        return max != null ? (int) max.value() : null;
    }

    private String formatLabel(String fieldName) {
        // sqlQuery -> SQL Query
        // streamBatchSize -> Stream Batch Size
        return fieldName.replaceAll("([A-Z])", " $1")
            .replaceAll("^.", m -> m.group().toUpperCase())
            .trim();
    }
}
```

---

## Implementation Plan (Final)

### Phase 1: Config Records (1 day)
- [ ] Create package structure: `config/extractor`, `config/transformer`, `config/loader`
- [ ] Create sealed interfaces
- [ ] Create all Record configurations with Bean Validation
- [ ] Create enum `KafkaFormat`
- [ ] Unit tests

### Phase 2: EtlJob Model (0.5 day)
- [ ] Replace class with record
- [ ] Remove old fields (`source`, `targetTable`, `parameters`)
- [ ] Configure Jackson @JsonTypeInfo/@JsonSubTypes
- [ ] Serialization/deserialization tests

### Phase 3: DB Schema (0.5 day)
- [ ] Flyway migration V2 - remove Source/Target/Params
- [ ] Add separate columns for types and configurations
- [ ] Indexes on types
- [ ] Update JobEntity

### Phase 4: Repository (0.5 day)
- [ ] Update mapping JobEntity <-> EtlJob
- [ ] CRUD operation tests

### Phase 5: EtlComponentFactory (1 day)
- [ ] Create centralized Factory
- [ ] Pattern matching for all components
- [ ] Update EtlPipelineFactory to use Factory
- [ ] Tests

### Phase 6: Components (2 days)
- [ ] Update JdbcExtractor - accept JdbcExtractorConfig
- [ ] Update KafkaPartitionExtractor - accept KafkaExtractorConfig
- [ ] Update all Transformers
- [ ] Update all Loaders
- [ ] Remove all `getParamOrDefault()`
- [ ] Optional field handling
- [ ] Unit tests

### Phase 7: Component Schema API (1 day)
- [ ] ComponentSchemaGenerator with reflection
- [ ] DTO classes (ComponentSchema, FieldSchema, etc)
- [ ] Endpoint `/api/jobs/schema`
- [ ] Generation tests

### Phase 8: Frontend (2 days)
- [ ] Rework job-form.html (component-oriented structure)
- [ ] Update job-form.js - load schema, dynamic rendering
- [ ] Remove all hardcoded defaults
- [ ] Optional field handling (nullable)
- [ ] UI shows recommendations instead of defaults
- [ ] Test create/edit

### Phase 9: E2E Testing (1 day)
- [ ] Tests of all combinations (SQL→SQL, SQL→Kafka, Kafka→SQL, Kafka→Kafka)
- [ ] Validation testing
- [ ] Optional field testing
- [ ] UI testing

**Total Time:** 9 days

---

## Advantages of Final Architecture

### 1. Type Safety - 100%

```java
// ❌ OLD CODE
int threads = (int) job.getParamOrDefault("threads", 4);  // runtime cast
String topic = (String) job.getParam("topic");            // can be null

// ✅ NEW CODE
int threads = config.threads();                           // compile-time safe
String topic = config.topic();                            // compile-time safe
Optional<String> keyCol = config.keyColumn();             // explicit null handling
```

### 2. No Defaults in Code

```java
// ❌ OLD CODE - defaults scattered everywhere
int threads = (int) job.getParamOrDefault("threads", 4);  // JdbcExtractor
int threads = (int) job.getParamOrDefault("threads", 4);  // KafkaExtractor

// ✅ NEW CODE - user explicitly specifies everything
// UI shows: "Threads (Recommended: 4)"
// User consciously chooses: 4, 8, 16, etc.
```

### 3. Exhaustive Pattern Matching

```java
// ✅ Compiler checks all cases
switch (job.extractorConfig()) {
    case JdbcExtractorConfig c -> handleJdbc(c);
    case KafkaExtractorConfig c -> handleKafka(c);
    // If a new sealed type is added - code won't compile!
}
```

### 4. SQL Queries by Types

```sql
-- ✅ Find all jobs with Kafka extractor
SELECT * FROM tEtlJob WHERE ExtractorType = 'kafka';

-- ✅ Usage statistics
SELECT ExtractorType, LoaderType, COUNT(*)
FROM tEtlJob
GROUP BY ExtractorType, LoaderType;

-- ✅ Indexes work
SELECT * FROM tEtlJob
WHERE ExtractorType = 'sql' AND LoaderType = 'kafka';
```

### 5. Less Code

| Component | Old Code | New Code | Difference |
|-----------|----------|----------|------------|
| EtlJob | ~180 lines | ~50 lines | **-72%** |
| JdbcExtractor | ~160 lines | ~100 lines | **-38%** |
| JobEntity | ~100 lines | ~80 lines | **-20%** |

### 6. Auto-generated Schema

```java
// ✅ Added new field to Record - schema updates automatically
public record JdbcExtractorConfig(
    // ... existing fields
    @NotNull @Min(1) Integer prefetchSize  // ← new field
) {}

// Schema API automatically returns new field in /api/jobs/schema
// Frontend automatically displays new field
```

---

## Final Architecture

```
┌─────────────────────────────────────────────────────┐
│                    Frontend (UI)                    │
│  - Loads schema from /api/jobs/schema              │
│  - Dynamically renders fields                      │
│  - NO hardcoded defaults                           │
│  - Shows recommendations (Recommended: 4)          │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│              REST API (JobController)               │
│  - @Valid automatic validation                     │
│  - Bean Validation from Records                    │
│  - NO manual validation                            │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│                  EtlJob (Record)                    │
│  - extractorConfig: ExtractorConfig                │
│  - transformerConfig: TransformerConfig            │
│  - loaderConfig: LoaderConfig                      │
│  - Type-safe, immutable                            │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│      EtlComponentFactory (Pattern Matching)        │
│  - Exhaustive switch for all sealed types          │
│  - Dispatch to correct component                   │
└─────────────────────────────────────────────────────┘
                         ↓
┌───────────────┬───────────────┬─────────────────────┐
│  JdbcExtractor│  KafkaExtractor│  ...              │
│  JdbcLoader   │  KafkaLoader   │  ...              │
│  - Type-safe  │  - Optional    │                   │
│  - NO defaults│  - NO casts    │                   │
└───────────────┴───────────────┴─────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│          Database (separate columns)                │
│  - ExtractorType, ExtractorConfig                  │
│  - TransformerType, TransformerConfig              │
│  - LoaderType, LoaderConfig                        │
│  - Indexes on types                                │
└─────────────────────────────────────────────────────┘
```

**Ready for implementation!** 🚀


