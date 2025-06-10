package ru.pospelov.etl.engine.model;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;

import java.sql.SQLException;
import java.sql.Types;
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

    private int mapJavaToSqlType(Object value) {
        if (value == null) return Types.VARCHAR;
        return switch (value.getClass().getSimpleName()) {
            case "Integer" -> Types.INTEGER;
            case "Long" -> Types.BIGINT;
            case "Double" -> Types.DOUBLE;
            case "Float" -> Types.FLOAT;
            case "BigDecimal" -> Types.DECIMAL;
            case "Boolean" -> Types.BOOLEAN;
            case "LocalDateTime", "Timestamp", "Instant" -> Types.TIMESTAMP;
            default -> Types.VARCHAR;
        };
    }
}
