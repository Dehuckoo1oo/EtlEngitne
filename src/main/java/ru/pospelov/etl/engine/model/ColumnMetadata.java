package ru.pospelov.etl.engine.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * Метаданные одной колонки из ResultSetMetaData.
 *
 * Используется для передачи типовой информации от JDBC extractor до loader'ов.
 * Позволяет loader'ам корректно конвертировать значения с учетом исходного SQL типа.
 *
 * <p>Примеры использования:
 * <ul>
 * <li>Определение необходимости специальной обработки (sql_variant)</li>
 * <li>Конвертация значений в JDBC-совместимые типы</li>
 * <li>Диагностика ошибок конвертации типов</li>
 * </ul>
 *
 * @see java.sql.ResultSetMetaData
 */
@AllArgsConstructor
@Getter
@ToString
public class ColumnMetadata {
    /**
     * Имя колонки (обычно из ResultSetMetaData.getColumnLabel).
     */
    private final String columnName;

    /**
     * JDBC тип колонки из java.sql.Types.
     * Например: Types.INTEGER, Types.VARCHAR, Types.TIMESTAMP, Types.OTHER.
     */
    private final int jdbcType;

    /**
     * Имя типа как в источнике данных (например SQL Server).
     * Примеры: "NVARCHAR", "DATETIME2", "DECIMAL", "sql_variant".
     *
     * Для sql_variant колонок это значение будет "sql_variant".
     */
    private final String typeName;

    /**
     * Precision (точность) для числовых и временных типов.
     * Для DECIMAL/NUMERIC: общее количество цифр.
     * Для строковых типов: максимальная длина.
     * Для временных типов: количество знаков после запятой в секундах.
     * 0 если не применимо.
     */
    private final int precision;

    /**
     * Scale (масштаб) для числовых типов.
     * Для DECIMAL/NUMERIC: количество цифр после десятичной точки.
     * 0 если не применимо.
     */
    private final int scale;

    /**
     * Допускает ли колонка NULL значения.
     */
    private final boolean nullable;
}
