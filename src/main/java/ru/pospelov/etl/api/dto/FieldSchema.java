package ru.pospelov.etl.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Schema for a single field in component configuration.
 * Auto-generated from Record components and validation annotations.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FieldSchema {

    /**
     * Field name (matches Record component name)
     */
    @JsonProperty("name")
    private String name;

    /**
     * Field type: text, number, boolean, enum, optional
     */
    @JsonProperty("type")
    private String type;

    /**
     * Human-readable label for UI
     */
    @JsonProperty("label")
    private String label;

    /**
     * Is this field required (from @NotNull/@NotBlank)
     */
    @JsonProperty("required")
    private Boolean required;

    /**
     * Minimum value (from @Min)
     */
    @JsonProperty("min")
    private Integer min;

    /**
     * Maximum value (from @Max)
     */
    @JsonProperty("max")
    private Integer max;

    /**
     * For enum types: list of possible values
     */
    @JsonProperty("enumValues")
    private List<String> enumValues;

    /**
     * Description or help text
     */
    @JsonProperty("description")
    private String description;
}
