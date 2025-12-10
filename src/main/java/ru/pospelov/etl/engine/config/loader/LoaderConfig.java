package ru.pospelov.etl.engine.config.loader;

public sealed interface LoaderConfig
    permits JdbcLoaderConfig, FastSqlLoaderConfig, KafkaLoaderConfig {
    String type();
}
