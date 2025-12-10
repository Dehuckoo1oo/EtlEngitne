package ru.pospelov.etl.engine.config.transformer;

public record AvroToRecordTransformerConfig() implements TransformerConfig {
    @Override
    public String type() {
        return "avro";
    }
}
