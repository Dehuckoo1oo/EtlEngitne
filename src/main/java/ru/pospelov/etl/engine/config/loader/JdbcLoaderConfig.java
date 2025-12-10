package ru.pospelov.etl.engine.config.loader;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record JdbcLoaderConfig(
    @NotBlank(message = "Target table is required")
    String targetTable,

    @NotNull(message = "Stream batch size is required")
    @Min(value = 100, message = "Stream batch size must be >= 100")
    @Max(value = 1_048_576, message = "Stream batch size must be <= 1,048,576 (2^20)")
    Integer streamBatchSize
) implements LoaderConfig {
    @Override
    public String type() {
        return "sql";
    }
}
