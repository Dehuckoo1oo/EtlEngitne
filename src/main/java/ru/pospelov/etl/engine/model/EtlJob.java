package ru.pospelov.etl.engine.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.pospelov.etl.engine.config.extractor.*;
import ru.pospelov.etl.engine.config.transformer.*;
import ru.pospelov.etl.engine.config.loader.*;

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
