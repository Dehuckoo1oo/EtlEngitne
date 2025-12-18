package ru.pospelov.etl.engine.steps.extractor.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;
import ru.pospelov.etl.engine.conversion.TypeConverter;
import ru.pospelov.etl.engine.model.ColumnMetadata;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.model.SqlVariantValue;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

/**
 * Streaming ResultSetExtractor that processes records in batches.
 * Выделен отдельно, чтобы связь с JdbcExtractor была явной.
 * <p>
 * Collects column metadata from ResultSetMetaData and passes it along with records
 * in EtlBatch for downstream type conversion.
 * <p>
 * <b>sql_variant Support:</b>
 * <ul>
 * <li>Detects sql_variant columns by presence of __variant_* metadata fields in ResultSet</li>
 * <li>Reads SQL_VARIANT_PROPERTY values (BaseType, Precision, Scale, MaxLength)</li>
 * <li>Creates SqlVariantValue objects with both base value and type metadata</li>
 * <li>Validates custom queries: warns if sql_variant column lacks SQL_VARIANT_PROPERTY</li>
 * </ul>
 */
final class JdbcStreamingResultSetExtractor implements ResultSetExtractor<Void> {

    private static final Logger log = LoggerFactory.getLogger(JdbcStreamingResultSetExtractor.class);

    private final String sourcePartition;
    private final int batchSize;
    private final String keyColumn;
    private final Consumer<EtlBatch> batchConsumer;
    private final TypeConverter typeConverter;
    private final String jobId;

    JdbcStreamingResultSetExtractor(String sourcePartition,
                                    int batchSize,
                                    String keyColumn,
                                    Consumer<EtlBatch> batchConsumer,
                                    TypeConverter typeConverter,
                                    String jobId) {
        this.sourcePartition = sourcePartition;
        this.batchSize = batchSize;
        this.keyColumn = keyColumn;
        this.batchConsumer = batchConsumer;
        this.typeConverter = typeConverter;
        this.jobId = jobId;
    }

    @Override
    public Void extractData(ResultSet rs) throws SQLException {
        rs.setFetchSize(10000);

        ResultSetMetaData md = rs.getMetaData();

        // Collect column metadata once for the entire ResultSet
        Map<String, ColumnMetadata> columnMetadata = buildColumnMetadata(md);

        // Detect sql_variant columns by __variant_* fields
        Map<String, SqlVariantColumnInfo> variantColumns = detectSqlVariantColumns(md, columnMetadata);

        // Validate: warn if sql_variant column exists but no __variant_* metadata
        validateSqlVariantMetadata(columnMetadata, variantColumns);

        List<EtlRecord> currentBatch = new ArrayList<>(batchSize);

        while (rs.next()) {
            EtlRecord record = new EtlRecord(Instant.now(), sourcePartition, rs.getRow());

            // Process all columns
            for (int i = 1; i <= md.getColumnCount(); i++) {
                String col = md.getColumnLabel(i);

                // Skip __variant_* metadata columns - they're already processed
                if (col.startsWith("__variant_")) {
                    continue;
                }

                ColumnMetadata colMeta = columnMetadata.get(col);
                Object val = rs.getObject(i);

                // Normalize JDBC types to canonical Java types
                val = normalizeJdbcValue(val, colMeta);

                // If this is a sql_variant column, create SqlVariantValue
                if (variantColumns.containsKey(col)) {
                    SqlVariantColumnInfo info = variantColumns.get(col);
                    val = createSqlVariantValue(rs, col, val, info);
                }

                record.put(col, val);
            }

            if (!keyColumn.isEmpty()) {
                // Use __kafka_key to avoid conflict with real SQL columns named "key"
                record.put("__kafka_key", rs.getObject(keyColumn));
            }
            currentBatch.add(record);

            if (currentBatch.size() >= batchSize) {
                batchConsumer.accept(new EtlBatch(new ArrayList<>(currentBatch), columnMetadata));
                currentBatch.clear();
            }
        }

        if (!currentBatch.isEmpty()) {
            batchConsumer.accept(new EtlBatch(currentBatch, columnMetadata));
        }

        return null;
    }

    /**
     * Build column metadata map from ResultSetMetaData.
     * This is called once per ResultSet, not per row, for efficiency.
     *
     * <p>Excludes __variant_* metadata columns as they are internal implementation detail
     * for sql_variant support and do not represent user data columns.
     */
    private Map<String, ColumnMetadata> buildColumnMetadata(ResultSetMetaData md) throws SQLException {
        Map<String, ColumnMetadata> metadata = new LinkedHashMap<>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            String colName = md.getColumnLabel(i);

            // Skip __variant_* metadata columns - they're internal implementation detail
            if (colName.startsWith("__variant_")) {
                continue;
            }

            metadata.put(colName, new ColumnMetadata(
                    colName,
                    md.getColumnType(i),
                    md.getColumnTypeName(i),
                    md.getPrecision(i),
                    md.getScale(i),
                    md.isNullable(i) == ResultSetMetaData.columnNullable
            ));
        }
        return metadata;
    }

    /**
     * Detect sql_variant columns by presence of __variant_* metadata fields in ResultSet.
     *
     * <p>For each column, checks if there are corresponding __variant_{column}_basetype,
     * _precision, _scale, _maxlength fields. If all 4 metadata fields are present,
     * the column is identified as sql_variant.
     *
     * @param md ResultSetMetaData
     * @param columnMetadata all column metadata
     * @return map of sql_variant column name to its metadata field info
     */
    private Map<String, SqlVariantColumnInfo> detectSqlVariantColumns(
            ResultSetMetaData md,
            Map<String, ColumnMetadata> columnMetadata) throws SQLException {

        Map<String, SqlVariantColumnInfo> variantColumns = new LinkedHashMap<>();

        // Build a set of all column names for fast lookup
        Set<String> allColumns = new HashSet<>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            allColumns.add(md.getColumnLabel(i));
        }

        // For each column, check if it has all 4 __variant_* metadata fields
        for (String colName : columnMetadata.keySet()) {
            // Skip __variant_* columns themselves
            if (colName.startsWith("__variant_")) {
                continue;
            }

            String baseTypeCol = "__variant_" + colName + "_basetype";
            String precisionCol = "__variant_" + colName + "_precision";
            String scaleCol = "__variant_" + colName + "_scale";
            String maxLengthCol = "__variant_" + colName + "_maxlength";

            // Check if all 4 metadata columns exist
            if (allColumns.contains(baseTypeCol) &&
                allColumns.contains(precisionCol) &&
                allColumns.contains(scaleCol) &&
                allColumns.contains(maxLengthCol)) {

                variantColumns.put(colName, new SqlVariantColumnInfo(
                        baseTypeCol, precisionCol, scaleCol, maxLengthCol
                ));

                log.debug("Detected sql_variant column: {} with metadata fields: {}, {}, {}, {}",
                        colName, baseTypeCol, precisionCol, scaleCol, maxLengthCol);
            }
        }

        if (!variantColumns.isEmpty()) {
            log.info("Found {} sql_variant column(s): {}", variantColumns.size(), variantColumns.keySet());
        }

        return variantColumns;
    }

    /**
     * Validate that sql_variant columns have proper SQL_VARIANT_PROPERTY metadata.
     * Warns if a column appears to be sql_variant (type -150 or "sql_variant") but lacks metadata.
     *
     * @param columnMetadata all column metadata
     * @param variantColumns detected sql_variant columns with metadata
     */
    private void validateSqlVariantMetadata(
            Map<String, ColumnMetadata> columnMetadata,
            Map<String, SqlVariantColumnInfo> variantColumns) {

        for (Map.Entry<String, ColumnMetadata> entry : columnMetadata.entrySet()) {
            String colName = entry.getKey();
            ColumnMetadata meta = entry.getValue();

            // Skip __variant_* metadata columns
            if (colName.startsWith("__variant_")) {
                continue;
            }

            // Check if column type is sql_variant (type -150 in JDBC or type name "sql_variant")
            boolean isSqlVariant = meta.getJdbcType() == -150 ||
                    "sql_variant".equalsIgnoreCase(meta.getTypeName());

            if (isSqlVariant && !variantColumns.containsKey(colName)) {
                log.warn("⚠️  Column '{}' is sql_variant but missing SQL_VARIANT_PROPERTY metadata fields. " +
                                "For table-based config, this is auto-generated. " +
                                "For custom query, manually add: " +
                                "SQL_VARIANT_PROPERTY({}, 'BaseType') as __variant_{}_basetype, " +
                                "SQL_VARIANT_PROPERTY({}, 'Precision') as __variant_{}_precision, " +
                                "SQL_VARIANT_PROPERTY({}, 'Scale') as __variant_{}_scale, " +
                                "SQL_VARIANT_PROPERTY({}, 'MaxLength') as __variant_{}_maxlength",
                        colName, colName, colName, colName, colName, colName, colName, colName, colName);
            }
        }
    }

    /**
     * Create SqlVariantValue from base value and SQL_VARIANT_PROPERTY metadata.
     *
     * @param rs ResultSet positioned at current row
     * @param colName column name
     * @param baseValue base value from the sql_variant column
     * @param info metadata field names
     * @return SqlVariantValue with base value and type metadata, or null if base value is null
     */
    private SqlVariantValue createSqlVariantValue(
            ResultSet rs,
            String colName,
            Object baseValue,
            SqlVariantColumnInfo info) throws SQLException {

        if (baseValue == null) {
            return null;
        }

        // Read SQL_VARIANT_PROPERTY values
        String baseType = rs.getString(info.baseTypeColumn);
        Integer precision = getIntegerOrNull(rs, info.precisionColumn);
        Integer scale = getIntegerOrNull(rs, info.scaleColumn);
        Integer maxLength = getIntegerOrNull(rs, info.maxLengthColumn);

        // Use TypeConverter to create SqlVariantValue with proper formatting
        return typeConverter.createSqlVariant(baseValue, baseType, precision, scale, maxLength, jobId);
    }

    /**
     * Normalizes JDBC value to canonical Java type according to SQL type.
     *
     * <p>This method ensures that regardless of the JDBC driver's specific behavior,
     * we always get consistent, canonical Java types for common SQL types.
     *
     * <p>Conversions:
     * <ul>
     * <li>{@link java.sql.Date} → {@link LocalDate}</li>
     * <li>{@link java.sql.Timestamp} → {@link Instant} (UTC) for DATETIME/DATETIME2/SMALLDATETIME</li>
     * <li>{@link java.sql.Time} → {@link LocalTime}</li>
     * <li>Driver-specific numeric → {@link java.math.BigDecimal} (for DECIMAL/NUMERIC/MONEY)</li>
     * </ul>
     *
     * @param value raw value from ResultSet.getObject()
     * @param meta column metadata
     * @return canonical Java type
     */
    private Object normalizeJdbcValue(Object value, ColumnMetadata meta) {
        if (value == null) {
            return null;
        }

        String typeName = meta.getTypeName().toLowerCase();

        // DATE → LocalDate
        if (typeName.equals("date") && value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate();
        }

        // DATETIME2/DATETIME/SMALLDATETIME → Instant (UTC)
        if ((typeName.equals("datetime2") || typeName.equals("datetime") || typeName.equals("smalldatetime"))
                && value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toInstant();
        }

        // TIME → LocalTime
        if (typeName.equals("time") && value instanceof java.sql.Time) {
            return ((java.sql.Time) value).toLocalTime();
        }

        // DECIMAL/NUMERIC/MONEY → BigDecimal
        if ((typeName.equals("decimal") || typeName.equals("numeric")
                || typeName.equals("money") || typeName.equals("smallmoney"))
                && !(value instanceof java.math.BigDecimal)) {
            // Handle driver-specific types
            if (value instanceof Number) {
                return new java.math.BigDecimal(value.toString());
            }
        }

        return value;
    }

    /**
     * Helper to get Integer from ResultSet, handling NULL.
     */
    private Integer getIntegerOrNull(ResultSet rs, String columnName) throws SQLException {
        int value = rs.getInt(columnName);
        return rs.wasNull() ? null : value;
    }

    /**
     * Internal class to hold sql_variant metadata field names for a column.
     */
    private record SqlVariantColumnInfo(
            String baseTypeColumn,
            String precisionColumn,
            String scaleColumn,
            String maxLengthColumn
    ) {}
}
