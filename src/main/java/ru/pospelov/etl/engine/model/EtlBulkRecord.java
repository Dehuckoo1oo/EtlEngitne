package ru.pospelov.etl.engine.model;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;

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

    public EtlBulkRecord(Collection<EtlRecord> records) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("Empty record set for bulk insert");
        }
        this.iterator = records.iterator();
        this.columns = new ArrayList<>(records.iterator().next().getAll().keySet());
        this.sqlTypes = detectColumnTypes(records.iterator().next());
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
        return sqlTypes.getOrDefault(getColumnName(column), Types.VARCHAR);
    }

    @Override
    public int getPrecision(int column) {
        return 0; // можно адаптировать при необходимости
    }

    @Override
    public int getScale(int column) {
        return 0; // можно адаптировать при необходимости
    }

    @Override
    public boolean next() throws SQLException {
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
            row[i] = current.get(columns.get(i));
        }
        return row;
    }

    private Map<String, Integer> detectColumnTypes(EtlRecord record) {
        Map<String, Integer> types = new HashMap<>();
        for (Map.Entry<String, Object> entry : record.getAll().entrySet()) {
            types.put(entry.getKey(), mapJavaToSqlType(entry.getValue()));
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
        if (value == null) {
            return Types.VARCHAR;
        }

        // Java time types (после конвертации через TypeConverter)
        if (value instanceof LocalDate) {
            return Types.DATE;
        }
        if (value instanceof Instant) {
            return Types.TIMESTAMP;
        }
        if (value instanceof LocalTime) {
            return Types.TIME;
        }
        if (value instanceof OffsetDateTime) {
            // SQL Server DATETIMEOFFSET не имеет прямого JDBC константы,
            // но Types.TIMESTAMP_WITH_TIMEZONE работает для большинства драйверов
            return Types.TIMESTAMP_WITH_TIMEZONE;
        }

        // Legacy SQL types (для обратной совместимости)
        if (value instanceof java.sql.Timestamp) {
            return Types.TIMESTAMP;
        }
        if (value instanceof java.sql.Date) {
            return Types.DATE;
        }
        if (value instanceof java.sql.Time) {
            return Types.TIME;
        }

        // Numeric types
        if (value instanceof Integer) {
            return Types.INTEGER;
        }
        if (value instanceof Long) {
            return Types.BIGINT;
        }
        if (value instanceof java.math.BigDecimal) {
            return Types.DECIMAL;
        }
        if (value instanceof Float) {
            return Types.FLOAT;
        }
        if (value instanceof Double) {
            return Types.DOUBLE;
        }

        // Boolean
        if (value instanceof Boolean) {
            return Types.BOOLEAN;
        }

        // Binary types
        if (value instanceof byte[]) {
            return Types.VARBINARY;
        }

        // String и все остальное
        return Types.VARCHAR;
    }
}
