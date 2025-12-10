package ru.pospelov.etl.engine.config.loader;

import jakarta.validation.constraints.NotBlank;

public record FastSqlLoaderConfig(
    @NotBlank(message = "Target table is required")
    String targetTable
) implements LoaderConfig {
    @Override
    public String type() {
        return "fast-sql";
    }
}
