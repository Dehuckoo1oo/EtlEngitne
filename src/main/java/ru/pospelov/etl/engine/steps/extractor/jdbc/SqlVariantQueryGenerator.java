package ru.pospelov.etl.engine.steps.extractor.jdbc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Генератор SQL запросов с автоматическим добавлением SQL_VARIANT_PROPERTY
 * для table-based конфигураций JDBC extractor.
 *
 * <p>Этот класс:
 * <ul>
 * <li>Запрашивает INFORMATION_SCHEMA.COLUMNS для получения списка колонок таблицы</li>
 * <li>Определяет sql_variant колонки по типу данных</li>
 * <li>Генерирует SELECT с дополнительными SQL_VARIANT_PROPERTY колонками</li>
 * </ul>
 *
 * <p>Пример сгенерированного SQL:
 * <pre>
 * SELECT
 *   order_id,
 *   customer_name,
 *   metadata_col,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'BaseType') as __variant_metadata_col_basetype,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'Precision') as __variant_metadata_col_precision,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'Scale') as __variant_metadata_col_scale,
 *   SQL_VARIANT_PROPERTY(metadata_col, 'MaxLength') as __variant_metadata_col_maxlength
 * FROM dbo.orders
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SqlVariantQueryGenerator {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Генерирует SELECT запрос для таблицы с автоматическим добавлением
     * SQL_VARIANT_PROPERTY колонок для всех sql_variant полей.
     *
     * <p>Если таблица не содержит sql_variant колонок, возвращает простой SELECT *.
     *
     * @param tableName имя таблицы (может включать схему: dbo.orders или просто orders)
     * @return сгенерированный SQL запрос
     */
    public String generateSelectQuery(String tableName) {
        // Parse schema and table name
        String schema = "dbo"; // default schema for SQL Server
        String table = tableName;

        if (tableName.contains(".")) {
            String[] parts = tableName.split("\\.", 2);
            schema = parts[0];
            table = parts[1];
        }

        log.debug("Generating SELECT query for table: schema={}, table={}", schema, table);

        // Query INFORMATION_SCHEMA to get all columns and identify sql_variant columns
        List<ColumnInfo> columns = queryInformationSchema(schema, table);

        if (columns.isEmpty()) {
            log.warn("No columns found for table {}.{}, returning simple SELECT *", schema, table);
            return "SELECT * FROM " + tableName;
        }

        // Check if there are any sql_variant columns
        boolean hasVariantColumns = columns.stream()
                .anyMatch(col -> "sql_variant".equalsIgnoreCase(col.dataType));

        if (!hasVariantColumns) {
            log.debug("No sql_variant columns found in {}.{}, returning simple SELECT *", schema, table);
            return "SELECT * FROM " + tableName;
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

        sql.append("\nFROM ").append(tableName);

        String generatedQuery = sql.toString();
        log.info("Generated SQL query for table {} with {} sql_variant columns:\n{}",
                tableName,
                columns.stream().filter(c -> "sql_variant".equalsIgnoreCase(c.dataType)).count(),
                generatedQuery);

        return generatedQuery;
    }

    /**
     * Запрашивает INFORMATION_SCHEMA.COLUMNS для получения информации о колонках таблицы.
     *
     * @param schema имя схемы (например, dbo)
     * @param table имя таблицы
     * @return список колонок с их типами, отсортированный по ORDINAL_POSITION
     */
    private List<ColumnInfo> queryInformationSchema(String schema, String table) {
        String query = """
                SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """;

        try {
            return jdbcTemplate.query(
                    query,
                    (rs, rowNum) -> new ColumnInfo(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("DATA_TYPE"),
                            rs.getInt("ORDINAL_POSITION")
                    ),
                    schema,
                    table
            );
        } catch (Exception e) {
            log.error("Failed to query INFORMATION_SCHEMA for table {}.{}: {}", schema, table, e.getMessage(), e);
            throw new RuntimeException("Failed to generate SELECT query for table " + schema + "." + table, e);
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
