package ru.pospelov.etl.engine.config.extractor;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

/**
 * JDBC extractor configuration.
 *
 * All fields are required (NotNull), except Optional fields.
 * NO defaults - user explicitly specifies all values.
 */
public record JdbcExtractorConfig(
    @NotBlank(message = "SQL query is required")
    String sqlQuery,

    Optional<String> partitionColumn,

    @NotNull(message = "Partitions is required")
    @Min(value = 1, message = "Partitions must be >= 1")
    Integer partitions,

    Optional<String> keyColumn,

    @NotNull(message = "Threads is required")
    @Min(value = 1, message = "Threads must be >= 1")
    Integer threads,

    @NotNull(message = "Stream batch size is required")
    @Min(value = 100, message = "Stream batch size must be >= 100")
    @Max(value = 1_048_576, message = "Stream batch size must be <= 1,048,576 (2^20)")
    Integer streamBatchSize
) implements ExtractorConfig {

    @Override
    public String type() {
        return "sql";
    }
}
