package ru.pospelov.etl.engine.config.extractor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

/**
 * JDBC extractor configuration.
 *
 * <p>Supports two modes:
 * <ul>
 * <li><b>Custom query mode:</b> Specify {@code sqlQuery} with custom SQL</li>
 * <li><b>Table-based mode:</b> Specify {@code table} for automatic query generation
 *     with sql_variant support</li>
 * </ul>
 *
 * <p>Exactly one of {@code sqlQuery} or {@code table} must be specified.
 *
 * <h2>Table-based mode</h2>
 * When {@code table} is specified, the system automatically:
 * <ul>
 * <li>Queries INFORMATION_SCHEMA to detect sql_variant columns</li>
 * <li>Generates SELECT with SQL_VARIANT_PROPERTY for each sql_variant column</li>
 * <li>No manual query writing needed</li>
 * </ul>
 *
 * <h2>Custom query mode</h2>
 * When {@code sqlQuery} is specified:
 * <ul>
 * <li>Query is used as-is</li>
 * <li>If query returns sql_variant columns, you must manually add SQL_VARIANT_PROPERTY columns</li>
 * <li>Validation will check for missing __variant_* columns (Stage 7)</li>
 * </ul>
 */
public record JdbcExtractorConfig(
    /**
     * Custom SQL query (SELECT statement).
     * Either this or {@code table} must be specified, but not both.
     */
    Optional<String> sqlQuery,

    /**
     * Table name for automatic query generation (supports schema: "dbo.orders").
     * Either this or {@code sqlQuery} must be specified, but not both.
     */
    Optional<String> table,

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

    /**
     * Validates that exactly one of sqlQuery or table is specified.
     */
    public JdbcExtractorConfig {
        if (sqlQuery.isEmpty() && table.isEmpty()) {
            throw new IllegalArgumentException("Either 'sqlQuery' or 'table' must be specified in JDBC extractor config");
        }
        if (sqlQuery.isPresent() && table.isPresent()) {
            throw new IllegalArgumentException("Cannot specify both 'sqlQuery' and 'table' in JDBC extractor config - use only one");
        }
    }

    @Override
    public String type() {
        return "sql";
    }

    /**
     * Returns the effective query to execute.
     * This is either the custom sqlQuery or the table name (for later processing).
     */
    @JsonIgnore
    public String getQueryOrTable() {
        return sqlQuery.orElseGet(() -> table.orElseThrow());
    }

    /**
     * Returns true if this is table-based configuration (auto-generation mode).
     */
    @JsonIgnore
    public boolean isTableBased() {
        return table.isPresent();
    }
}
