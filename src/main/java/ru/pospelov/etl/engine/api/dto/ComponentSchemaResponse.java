package ru.pospelov.etl.engine.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Complete schema response for all ETL components.
 * Maps component type to its configuration schema.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ComponentSchemaResponse {

    /**
     * Map of extractor type -> schema
     * e.g., "sql" -> JdbcExtractorConfig schema
     */
    @JsonProperty("extractors")
    private Map<String, ComponentSchema> extractors;

    /**
     * Map of transformer type -> schema
     * e.g., "noop" -> NoopTransformerConfig schema
     */
    @JsonProperty("transformers")
    private Map<String, ComponentSchema> transformers;

    /**
     * Map of loader type -> schema
     * e.g., "kafka" -> KafkaLoaderConfig schema
     */
    @JsonProperty("loaders")
    private Map<String, ComponentSchema> loaders;
}
