package ru.pospelov.etl.engine.config.extractor;

/**
 * Base configuration for all extractors.
 * Sealed - we can only add new types explicitly.
 */
public sealed interface ExtractorConfig
    permits JdbcExtractorConfig, KafkaExtractorConfig {
    String type();
}
