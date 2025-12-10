package ru.pospelov.etl.engine.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Schema for a single ETL component (extractor, transformer, or loader).
 * Describes all configuration fields for this component type.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ComponentSchema {

    /**
     * Human-readable display name for UI
     */
    @JsonProperty("displayName")
    private String displayName;

    /**
     * List of configuration fields for this component
     */
    @JsonProperty("fields")
    private List<FieldSchema> fields;
}
