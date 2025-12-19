package ru.pospelov.etl.engine.model;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import ru.pospelov.etl.engine.conversion.TypeConverter;

import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.*;

public class EtlBulkRecord implements ISQLServerBulkData {

    private final Iterator<EtlRecord> iterator;
    private EtlRecord current;
    private final List<String> columns;
    private final Map<String, Integer> sqlTypes;
    private final Map<String, ColumnMetadata> columnMetadata;
    private final TypeConverter typeConverter;
    private final Set<String> sqlVariantColumns;  // Columns that contain sql_variant
    private final Map<String, SqlVariantPrecisionScale> sqlVariantPrecisionScale;

    public EtlBulkRecord(Collection<EtlRecord> records) {
        this(records, null);
    }

    public EtlBulkRecord(Collection<EtlRecord> records, Map<String, ColumnMetadata> columnMetadata) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("Empty record set for bulk insert");
        }
        this.iterator = records.iterator();
        EtlRecord firstRecord = records.iterator().next();
        this.columns = new ArrayList<>(firstRecord.getAll().keySet());
        this.columnMetadata = columnMetadata;
        // Detect sql_variant columns from metadata first, then from actual data
        this.sqlVariantColumns = detectSqlVariantColumns(firstRecord, columnMetadata);
        this.sqlTypes = detectColumnTypes(firstRecord);
        this.sqlVariantPrecisionScale = detectSqlVariantPrecisionScale(firstRecord);
        this.typeConverter = new TypeConverter();  // For unpacking SqlVariantValue
    }

    /**
     * Detect which columns contain sql_variant values.
     *
     * <p>First checks column metadata (if available) to identify sql_variant columns
     * by their type name. If metadata is not available or doesn't indicate sql_variant,
     * falls back to checking if the actual value is a SqlVariantValue instance.
     *
     * <p>Note: After unpacking in FastSqlServerLoader, SqlVariantValue objects are converted
     * to base types (Integer, Long, BigDecimal, etc.), so metadata is the primary source
     * for identifying sql_variant columns.
     */
    private Set<String> detectSqlVariantColumns(EtlRecord record, Map<String, ColumnMetadata> metadata) {
        Set<String> variantCols = new HashSet<>();

        // First, check metadata for sql_variant type
        if (metadata != null) {
            for (Map.Entry<String, ColumnMetadata> entry : metadata.entrySet()) {
                if ("sql_variant".equalsIgnoreCase(entry.getValue().getTypeName())) {
                    variantCols.add(entry.getKey());
                }
            }
        }

        // Also check actual data (for cases where metadata is missing)
        for (Map.Entry<String, Object> entry : record.getAll().entrySet()) {
            if (entry.getValue() instanceof SqlVariantValue) {
                variantCols.add(entry.getKey());
            }
        }

        return variantCols;
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        Set<Integer> ordinals = new LinkedHashSet<>();
        for (int i = 1; i <= columns.size(); i++) ordinals.add(i);
        return ordinals;
    }

    @Override
    public String getColumnName(int column) {
        return columns.get(column - 1);
    }

    @Override
    public int getColumnType(int column) {
        String columnName = getColumnName(column);
        return sqlTypes.getOrDefault(columnName, Types.VARCHAR);
    }

    @Override
    public int getPrecision(int column) {
        String columnName = getColumnName(column);
        if (sqlVariantColumns.contains(columnName)) {
            SqlVariantPrecisionScale precisionScale = sqlVariantPrecisionScale.get(columnName);
            return precisionScale != null ? precisionScale.precision() : 0;
        }
        if (columnMetadata == null) {
            return 0;
        }
        ColumnMetadata meta = columnMetadata.get(columnName);
        return meta != null ? meta.getPrecision() : 0;
    }

    @Override
    public int getScale(int column) {
        String columnName = getColumnName(column);
        if (sqlVariantColumns.contains(columnName)) {
            SqlVariantPrecisionScale precisionScale = sqlVariantPrecisionScale.get(columnName);
            return precisionScale != null ? precisionScale.scale() : 0;
        }
        if (columnMetadata == null) {
            return 0;
        }
        ColumnMetadata meta = columnMetadata.get(columnName);
        return meta != null ? meta.getScale() : 0;
    }

    @Override
    public boolean next() {
        if (iterator.hasNext()) {
            current = iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public Object[] getRowData() throws SQLException {
        Object[] row = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Object value = current.get(columns.get(i));
            // Unpack SqlVariantValue to base type for bulk copy
            if (value instanceof SqlVariantValue) {
                try {
                    value = typeConverter.unpackSqlVariant((SqlVariantValue) value, "bulk-copy");
                } catch (Exception e) {
                    throw new SQLException("Failed to unpack sql_variant: " + e.getMessage(), e);
                }
            }
            row[i] = value;
        }
        return row;
    }

    private Map<String, Integer> detectColumnTypes(EtlRecord record) {
        Map<String, Integer> types = new HashMap<>();
        for (Map.Entry<String, Object> entry : record.getAll().entrySet()) {
            Object value = entry.getValue();
            if (value instanceof SqlVariantValue) {
                types.put(entry.getKey(), mapSqlVariantToSqlType((SqlVariantValue) value));
            } else {
                types.put(entry.getKey(), mapJavaToSqlType(value));
            }
        }
        return types;
    }

    /**
     * Определяет JDBC тип на основе Java типа значения.
     *
     * <p>Поддерживает все типы из type-conversion matrix, включая:
     * <ul>
     * <li>Java time types: LocalDate, Instant, LocalTime, OffsetDateTime</li>
     * <li>Numeric types: Integer, Long, BigDecimal, Float, Double</li>
     * <li>Text types: String</li>
     * <li>Binary types: byte[]</li>
     * <li>Boolean</li>
     * </ul>
     *
     * <p>Важно: sql_variant значения должны быть распакованы через TypeConverter.unpackSqlVariant()
     * перед передачей в bulk copy, поэтому здесь обрабатываются только базовые типы.
     *
     * @param value Java значение
     * @return JDBC type constant из java.sql.Types
     */
    private int mapJavaToSqlType(Object value) {
        return switch (value) {
            case null -> Types.VARCHAR;


            // Java time types (после конвертации через TypeConverter)
            case LocalDate localDate -> Types.DATE;
            case Instant instant -> Types.TIMESTAMP;
            case LocalTime localTime -> Types.TIME;
            case OffsetDateTime offsetDateTime ->
                // SQL Server DATETIMEOFFSET не имеет прямого JDBC константы,
                // но Types.TIMESTAMP_WITH_TIMEZONE работает для большинства драйверов
                    Types.TIMESTAMP_WITH_TIMEZONE;


            // Legacy SQL types (для обратной совместимости)
            case java.sql.Timestamp timestamp -> Types.TIMESTAMP;
            case java.sql.Date date -> Types.DATE;
            case java.sql.Time time -> Types.TIME;


            // Numeric types
            case Integer i -> Types.INTEGER;
            case Long l -> Types.BIGINT;
            case java.math.BigDecimal bigDecimal -> Types.DECIMAL;
            case Float v -> Types.FLOAT;
            case Double v -> Types.DOUBLE;


            // Boolean
            case Boolean b -> Types.BOOLEAN;


            // Binary types
            case byte[] bytes -> Types.VARBINARY;


            // Short (from TINYINT/SMALLINT)
            case Short i -> Types.SMALLINT;


            // Byte (from TINYINT unsigned)
            case Byte b -> Types.TINYINT;
            default ->

                // String и все остальное
                    Types.VARCHAR;
        };

    }

    private static record SqlVariantPrecisionScale(int precision, int scale) {
    }

    private Map<String, SqlVariantPrecisionScale> detectSqlVariantPrecisionScale(EtlRecord record) {
        Map<String, SqlVariantPrecisionScale> precisionScale = new HashMap<>();
        for (String columnName : sqlVariantColumns) {
            Object value = record.get(columnName);
            if (value instanceof SqlVariantValue) {
                precisionScale.put(columnName,
                        parseSqlVariantPrecisionScale((SqlVariantValue) value));
            } else {
                precisionScale.put(columnName, parseSqlVariantPrecisionScale(null));
            }
        }
        return precisionScale;
    }

    private SqlVariantPrecisionScale parseSqlVariantPrecisionScale(SqlVariantValue variant) {
        if (variant == null || variant.getSqlType() == null) {
            return new SqlVariantPrecisionScale(1, 0);
        }

        String sqlType = variant.getSqlType();
        String normalized = sqlType.toLowerCase().trim();
        String baseType = extractBaseType(normalized);
        String params = extractTypeParams(normalized);

        return switch (baseType) {
            case "decimal", "numeric" -> parseDecimalPrecisionScale(params);
            case "money" -> new SqlVariantPrecisionScale(19, 4);
            case "smallmoney" -> new SqlVariantPrecisionScale(10, 4);
            case "varchar", "nvarchar", "char", "nchar", "varbinary", "binary" -> new SqlVariantPrecisionScale(
                    parseLengthParam(params, 1), 0
            );
            case "text", "ntext", "xml", "image" -> new SqlVariantPrecisionScale(-1, 0);
            case "rowversion", "timestamp" -> new SqlVariantPrecisionScale(8, 0);
            case "float" -> new SqlVariantPrecisionScale(53, 0);
            case "real" -> new SqlVariantPrecisionScale(24, 0);
            case "date", "time", "datetime", "datetime2", "datetimeoffset", "smalldatetime" -> {
                int length = temporalStringLength(baseType, params, variant);
                yield new SqlVariantPrecisionScale(length, 0);
            }
            case "uniqueidentifier" -> new SqlVariantPrecisionScale(
                    resolveStringLength(variant, 36), 0
            );
            default -> new SqlVariantPrecisionScale(1, 0);
        };
    }

    private SqlVariantPrecisionScale parseDecimalPrecisionScale(String params) {
        if (params == null || params.isBlank()) {
            return new SqlVariantPrecisionScale(18, 0);
        }

        String[] parts = params.split(",");
        int precision = parseNumericParam(parts[0], 18);
        int scale = parts.length > 1 ? parseNumericParam(parts[1], 0) : 0;
        return new SqlVariantPrecisionScale(precision, scale);
    }

    private int parseLengthParam(String params, int defaultLength) {
        if (params == null || params.isBlank()) {
            return defaultLength;
        }
        String trimmed = params.trim();
        if ("max".equalsIgnoreCase(trimmed)) {
            return -1;
        }
        return parseNumericParam(trimmed, defaultLength);
    }

    private int parseNumericParam(String value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private int resolveStringLength(SqlVariantValue variant, int defaultLength) {
        String value = variant.getValue();
        if (value == null) {
            return defaultLength;
        }
        int length = value.length();
        return length > 0 ? length : defaultLength;
    }

    private int temporalStringLength(String baseType, String params, SqlVariantValue variant) {
        int defaultScale = switch (baseType) {
            case "time", "datetime2", "datetimeoffset" -> 7;
            case "datetime" -> 3;
            default -> 0;
        };
        int scale = parseNumericParam(params, defaultScale);

        int baseLength = switch (baseType) {
            case "date" -> 10; // yyyy-MM-dd
            case "time" -> 8 + (scale > 0 ? 1 + scale : 0); // HH:mm:ss[.fffffff]
            case "datetime2", "datetime" -> 19 + (scale > 0 ? 1 + scale : 0); // yyyy-MM-dd HH:mm:ss[.fffffff]
            case "datetimeoffset" -> 26 + (scale > 0 ? 1 + scale : 0); // yyyy-MM-dd HH:mm:ss[.fffffff] +HH:mm
            case "smalldatetime" -> 21; // Timestamp.toString() can include ".0"
            default -> 30;
        };

        return resolveStringLength(variant, baseLength);
    }

    private String extractTypeParams(String sqlType) {
        int start = sqlType.indexOf('(');
        if (start < 0) {
            return null;
        }
        int end = sqlType.indexOf(')', start + 1);
        if (end < 0) {
            return null;
        }
        return sqlType.substring(start + 1, end).trim();
    }

    private int mapSqlVariantToSqlType(SqlVariantValue variant) {
        if (variant == null || variant.getSqlType() == null) {
            return Types.VARCHAR;
        }

        String baseType = extractBaseType(variant.getSqlType().toLowerCase());
        return switch (baseType) {
            case "int" -> Types.INTEGER;
            case "smallint" -> Types.SMALLINT;
            case "tinyint" -> Types.TINYINT;
            case "bigint" -> Types.BIGINT;
            case "bit" -> Types.BIT;
            case "real" -> Types.REAL;
            case "float" -> Types.DOUBLE;
            case "decimal", "numeric", "money", "smallmoney" -> Types.DECIMAL;
            case "varchar", "char", "text", "xml" -> Types.VARCHAR;
            case "nvarchar", "nchar", "ntext" -> Types.NVARCHAR;
            case "varbinary", "binary", "image", "rowversion", "timestamp" -> Types.VARBINARY;
            case "date", "time", "datetime", "datetime2", "smalldatetime", "datetimeoffset" -> Types.VARCHAR;
            case "uniqueidentifier" -> Types.VARCHAR;
            default -> Types.VARCHAR;
        };
    }

    private String extractBaseType(String sqlType) {
        int parenIndex = sqlType.indexOf('(');
        return parenIndex > 0 ? sqlType.substring(0, parenIndex) : sqlType;
    }
}
