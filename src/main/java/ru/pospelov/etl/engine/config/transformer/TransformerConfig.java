package ru.pospelov.etl.engine.config.transformer;

public sealed interface TransformerConfig
    permits NoopTransformerConfig, AvroToRecordTransformerConfig, RecordToAvroTransformerConfig {
    String type();
}
