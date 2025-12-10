package ru.pospelov.etl.engine.config;

/**
 * Kafka data format - type-safe enum instead of String.
 */
public enum KafkaFormat {
    AVRO,
    JSON,
    STRING
}
