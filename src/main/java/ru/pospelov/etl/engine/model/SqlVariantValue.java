package ru.pospelov.etl.engine.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Контейнер для значения типа SQL_VARIANT с полной информацией о базовом типе.
 *
 * <h2>Зачем нужен этот класс</h2>
 * SQL Server тип sql_variant может содержать значения различных базовых типов
 * (int, decimal, datetime2, varchar, varbinary и т.д.). Базовый тип может отличаться
 * в каждой строке таблицы. ResultSetMetaData сообщает только что колонка имеет тип
 * "sql_variant", но не сообщает базовый тип конкретного значения.
 *
 * <h2>Жизненный цикл в ETL pipeline</h2>
 * <ol>
 * <li><b>JDBC Extractor</b>: читает sql_variant колонку + SQL_VARIANT_PROPERTY метаданные,
 *     создает SqlVariantValue через {@code TypeConverter.createSqlVariant()}</li>
 * <li><b>Kafka Loader (format=AVRO)</b>: сериализует в JSON-строку через
 *     {@code TypeConverter.sqlVariantToJson()}:
 *     <pre>{"v":1,"t":"decimal(18,2)","val":"12.30","enc":"plain"}</pre></li>
 * <li><b>Kafka Extractor</b>: десериализует JSON обратно в SqlVariantValue через
 *     {@code TypeConverter.sqlVariantFromJson()}</li>
 * <li><b>SQL Loader</b>: распаковывает в базовый Java-тип через
 *     {@code TypeConverter.unpackSqlVariant()} для JDBC вставки
 *     (Integer/BigDecimal/byte[]/etc.)</li>
 * </ol>
 *
 * <h2>Формат полей</h2>
 * <dl>
 * <dt><b>sqlType</b></dt>
 * <dd>Точная строка типа как в SQL Server. Примеры:
 *     <ul>
 *     <li>{@code "int"} - целое число</li>
 *     <li>{@code "decimal(18,2)"} - decimal с precision=18, scale=2</li>
 *     <li>{@code "datetime2(7)"} - datetime2 с точностью 7 знаков после запятой</li>
 *     <li>{@code "varchar(50)"} - varchar с максимальной длиной 50</li>
 *     <li>{@code "varbinary(max)"} - бинарные данные переменной длины</li>
 *     </ul>
 * </dd>
 *
 * <dt><b>value</b></dt>
 * <dd>Каноническое строковое представление значения.
 *     Для бинарных данных используется encoding base64/hex.</dd>
 *
 * <dt><b>encoding</b></dt>
 * <dd>Кодирование значения:
 *     <ul>
 *     <li>{@code "plain"} - обычное строковое представление (по умолчанию)</li>
 *     <li>{@code "base64"} - Base64 кодирование для бинарных данных</li>
 *     <li>{@code "hex"} - HEX кодирование для бинарных данных</li>
 *     </ul>
 * </dd>
 * </dl>
 *
 * <h2>JSON формат для Avro</h2>
 * Сериализуется в JSON для хранения в Avro string поле:
 * <pre>
 * {"v":1,"t":"decimal(18,2)","val":"12.30","enc":"plain"}
 *
 * Поля JSON:
 * - v:   версия формата (обязательна для будущей совместимости)
 * - t:   sqlType
 * - val: value
 * - enc: encoding
 * </pre>
 *
 * <h2>Примеры</h2>
 * <h3>Integer значение</h3>
 * <pre>
 * SqlVariantValue variant = new SqlVariantValue("int", "123", "plain");
 * // JSON: {"v":1,"t":"int","val":"123","enc":"plain"}
 * </pre>
 *
 * <h3>Decimal значение</h3>
 * <pre>
 * SqlVariantValue variant = new SqlVariantValue("decimal(18,2)", "1234.56", "plain");
 * // JSON: {"v":1,"t":"decimal(18,2)","val":"1234.56","enc":"plain"}
 * </pre>
 *
 * <h3>Datetime2 значение</h3>
 * <pre>
 * SqlVariantValue variant = new SqlVariantValue("datetime2(7)", "2024-01-15T10:30:45.1234567", "plain");
 * // JSON: {"v":1,"t":"datetime2(7)","val":"2024-01-15T10:30:45.1234567","enc":"plain"}
 * </pre>
 *
 * <h3>Binary значение</h3>
 * <pre>
 * SqlVariantValue variant = new SqlVariantValue("varbinary(100)", "AQIDBA==", "base64");
 * // JSON: {"v":1,"t":"varbinary(100)","val":"AQIDBA==","enc":"base64"}
 * </pre>
 *
 * <h2>Ограничения при bulk copy</h2>
 * При вставке через bulk copy в SQL Server, строковые базовые типы автоматически
 * конвертируются:
 * <ul>
 * <li>{@code varchar} → {@code nvarchar}</li>
 * <li>{@code char} → {@code nchar}</li>
 * </ul>
 *
 * Это ограничение bulk copy API, точное восстановление требует PreparedStatement
 * с CAST (не используется из-за производительности).
 *
 * <p>Функционально {@code nvarchar} полностью включает {@code varchar}
 * (Unicode superset of ANSI). Потеря только в памяти (2 байта вместо 1 на символ)
 * внутри sql_variant.
 *
 * <p>Числовые типы ({@code int}, {@code bigint}, {@code decimal(p,s)}),
 * даты/время ({@code date}, {@code datetime2(p)}), и бинарные ({@code varbinary(n)})
 * восстанавливаются корректно.
 *
 * @see ru.pospelov.etl.engine.conversion.TypeConverter#createSqlVariant
 * @see ru.pospelov.etl.engine.conversion.TypeConverter#sqlVariantToJson
 * @see ru.pospelov.etl.engine.conversion.TypeConverter#unpackSqlVariant
 * @see <a href="https://learn.microsoft.com/en-us/sql/t-sql/data-types/sql-variant-transact-sql">SQL Server sql_variant documentation</a>
 */
@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
public class SqlVariantValue {
    /**
     * Точный базовый тип как в SQL Server.
     *
     * <p>Примеры: {@code "int"}, {@code "decimal(18,2)"}, {@code "datetime2(7)"},
     * {@code "varchar(50)"}, {@code "varbinary(max)"}
     *
     * <p>Формируется из значений SQL_VARIANT_PROPERTY:
     * <ul>
     * <li>BaseType: {@code "int"}, {@code "decimal"}, {@code "datetime2"}, etc.</li>
     * <li>Precision: добавляется для decimal, datetime2</li>
     * <li>Scale: добавляется для decimal</li>
     * <li>MaxLength: добавляется для varchar, varbinary</li>
     * </ul>
     */
    private final String sqlType;

    /**
     * Каноническое строковое представление значения.
     *
     * <p>Для текстовых и числовых типов - обычное строковое представление.
     * <p>Для бинарных данных - кодируется согласно полю {@link #encoding}.
     * <p>Для дат/времени - ISO 8601 формат.
     */
    private final String value;

    /**
     * Кодирование значения: {@code "plain"}, {@code "base64"}, {@code "hex"}.
     *
     * <p>По умолчанию {@code "plain"} для всех типов кроме бинарных.
     * <p>Для {@code varbinary} типов используется {@code "base64"} (предпочтительно)
     * или {@code "hex"}.
     */
    private final String encoding;
}
