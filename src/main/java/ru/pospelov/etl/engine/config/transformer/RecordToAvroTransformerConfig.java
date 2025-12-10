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
