package ru.pospelov.etl.engine.steps.extractor.jdbc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * SQL query generator with automatic SQL_VARIANT_PROPERTY column addition
 * for table-based JDBC extractor configurations.
 *
 * <p>Features:
 * <ul>
 * <li>Queries INFORMATION_SCHEMA.COLUMNS to get table column list</li>
 * <li>Identifies sql_variant columns by data type</li>
 * <li>Generates SELECT with additional SQL_VARIANT_PROPERTY columns</li>
 * <li>Supports cross-database queries (database.schema.table format)</li>
 * </ul>
 *
 * <p>Supported table name formats:
 * <ul>
 * <li>"table" → uses default schema (dbo), current database</li>
 * <li>"schema.table" → uses current database</li>
 * <li>"database.schema.table" → cross-database query</li>
 * </ul>
 *
 * <p>Example generated SQL:
 * <pre>
 * SELECT
 *   order_id,
 *   customer_name,
 *   metadata_col,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'BaseType') as __variant_metadata_col_basetype,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'Precision') as __variant_metadata_col_precision,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'Scale') as __variant_metadata_col_scale,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'MaxLength') as __variant_metadata_col_maxlength
 * FROM SUPPORT.dbo.orders
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SqlVariantQueryGenerator {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Parsed components of a table identifier.
     * Supports formats: table, schema.table, database.schema.table
     *
     * @param database Database name (null = current database)
     * @param schema Schema name (never null, defaults to "dbo")
     * @param table Table name (never null)
     * @param fullName Original full name for use in FROM clause
     */
    private record TableIdentifier(
            String database,
            String schema,
            String table,
            String fullName
    ) {}

    /**
     * Parses table name into database, schema, and table components.
     * Supports multiple formats:
     * <ul>
     * <li>"table" → database=null, schema="dbo", table="table"</li>
     * <li>"schema.table" → database=null, schema="schema", table="table"</li>
     * <li>"database.schema.table" → database="database", schema="schema", table="table"</li>
     * </ul>
     *
     * @param tableName Full table name (may include database and/or schema)
     * @return Parsed table identifier
     * @throws IllegalArgumentException if table name is invalid
     */
    private TableIdentifier parseTableName(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }

        tableName = tableName.trim();

        if (!tableName.contains(".")) {
            // Format: table
            return new TableIdentifier(null, "dbo", tableName, tableName);
        }

        String[] parts = tableName.split("\\.");

        if (parts.length == 2) {
            // Format: schema.table
            return new TableIdentifier(null, parts[0], parts[1], tableName);
        } else if (parts.length >= 3) {
            // Format: database.schema.table (or more parts - take last 3)
            String database = parts[parts.length - 3];
            String schema = parts[parts.length - 2];
            String table = parts[parts.length - 1];
            return new TableIdentifier(database, schema, table, tableName);
        }

        throw new IllegalArgumentException("Invalid table name format: " + tableName);
    }

    /**
     * Generates SELECT query for a table with automatic addition of
     * SQL_VARIANT_PROPERTY columns for all sql_variant fields.
     *
     * <p>If the table contains no sql_variant columns, returns simple SELECT *.
     *
     * <p>Supports cross-database queries when table name contains database prefix
     * (e.g., "SUPPORT.dbo.orders").
     *
     * @param tableName table name (can include database and/or schema: database.schema.table, schema.table, or just table)
     * @return generated SQL query
     */
    public String generateSelectQuery(String tableName) {
        // Parse table name into database, schema, and table components
        TableIdentifier tableId = parseTableName(tableName);

        log.debug("Generating SELECT query for table: database={}, schema={}, table={} (original={})",
                tableId.database, tableId.schema, tableId.table, tableId.fullName);

        // Query INFORMATION_SCHEMA to get all columns and identify sql_variant columns
        List<ColumnInfo> columns = queryInformationSchema(tableId);

        if (columns.isEmpty()) {
            log.warn("No columns found for table {}, returning simple SELECT *", tableId.fullName);
            return "SELECT * FROM " + tableId.fullName;
        }

        // Check if there are any sql_variant columns
        boolean hasVariantColumns = columns.stream()
                .anyMatch(col -> "sql_variant".equalsIgnoreCase(col.dataType));

        if (!hasVariantColumns) {
            log.debug("No sql_variant columns found in {}, returning simple SELECT *", tableId.fullName);
            return "SELECT * FROM " + tableId.fullName;
        }

        // Build SELECT with SQL_VARIANT_PROPERTY for variant columns
        StringBuilder sql = new StringBuilder("SELECT\n");
        boolean first = true;

        for (ColumnInfo col : columns) {
            if (!first) {
                sql.append(",\n");
            }
            first = false;

            // Add column itself
            sql.append("  ").append(col.columnName);

            // If it's sql_variant, add SQL_VARIANT_PROPERTY columns
            if ("sql_variant".equalsIgnoreCase(col.dataType)) {
                sql.append(",\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'BaseType') as __variant_").append(col.columnName).append("_basetype,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'Precision') as __variant_").append(col.columnName).append("_precision,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'Scale') as __variant_").append(col.columnName).append("_scale,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName)
                        .append(", 'MaxLength') as __variant_").append(col.columnName).append("_maxlength");
            }
        }

        sql.append("\nFROM ").append(tableId.fullName);

        String generatedQuery = sql.toString();
        log.info("Generated SQL query for table {} with {} sql_variant columns:\n{}",
                tableId.fullName,
                columns.stream().filter(c -> "sql_variant".equalsIgnoreCase(c.dataType)).count(),
                generatedQuery);

        return generatedQuery;
    }

    /**
     * Queries INFORMATION_SCHEMA.COLUMNS to get column information.
     * Supports cross-database queries by using database-qualified INFORMATION_SCHEMA reference.
     *
     * @param tableId Parsed table identifier with database, schema, and table
     * @return List of columns with their types, sorted by ORDINAL_POSITION
     */
    private List<ColumnInfo> queryInformationSchema(TableIdentifier tableId) {
        // Build INFORMATION_SCHEMA table reference
        // For cross-database: database.INFORMATION_SCHEMA.COLUMNS
        // For same-database: INFORMATION_SCHEMA.COLUMNS
        String infoSchemaTable;
        if (tableId.database != null) {
            infoSchemaTable = tableId.database + ".INFORMATION_SCHEMA.COLUMNS";
        } else {
            infoSchemaTable = "INFORMATION_SCHEMA.COLUMNS";
        }

        String query = String.format("""
                SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
                FROM %s
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """, infoSchemaTable);

        try {
            return jdbcTemplate.query(
                    query,
                    (rs, rowNum) -> new ColumnInfo(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("DATA_TYPE"),
                            rs.getInt("ORDINAL_POSITION")
                    ),
                    tableId.schema,
                    tableId.table
            );
        } catch (Exception e) {
            log.error("Failed to query INFORMATION_SCHEMA for table {}: {}",
                      tableId.fullName, e.getMessage(), e);
            throw new RuntimeException("Failed to generate SELECT query for table " + tableId.fullName, e);
        }
    }

    /**
     * Информация о колонке из INFORMATION_SCHEMA.
     */
    private record ColumnInfo(
            String columnName,
            String dataType,
            int ordinalPosition
    ) {}
}
