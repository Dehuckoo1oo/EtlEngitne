package ru.pospelov.etl.engine.conversion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlStage;
import ru.pospelov.etl.engine.exception.TypeConversionException;
import ru.pospelov.etl.engine.model.ColumnMetadata;
import ru.pospelov.etl.engine.model.SqlVariantValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Base64;

/**
 * Конвертер типов для ETL pipeline.
 *
 * <p>Выполняет приведение типов между Java, Avro и JDBC представлениями.
 * Не зависит от инфраструктуры Kafka/SQL - только чистая логика конвертации.
 *
 * <h2>Основные функции</h2>
 * <ul>
 * <li>Конвертация Java типов в Avro-совместимые физические типы (SQL → Kafka)</li>
 * <li>Конвертация из Avro логических типов обратно в JDBC-совместимые типы (Kafka → SQL)</li>
 * <li>Обработка sql_variant: создание, упаковка, распаковка, JSON-сериализация</li>
 * </ul>
 *
 * <h2>Правила конвертации</h2>
 *
 * <h3>Java → Avro (для Kafka loader с format=AVRO)</h3>
 * <ul>
 * <li>{@link LocalDate} → {@code int} (epoch days) для {@code logicalType: "date"}</li>
 * <li>{@link Instant} → {@code long} (epoch millis) для {@code logicalType: "timestamp-millis"}</li>
 * <li>{@link LocalTime} → {@code long} (microseconds) для {@code logicalType: "time-micros"}</li>
 * <li>{@link BigDecimal} → {@link ByteBuffer} для {@code logicalType: "decimal"}</li>
 * <li>{@link SqlVariantValue} → {@link String} (через JSON)</li>
 * <li>{@code byte[]} → {@link ByteBuffer} для Avro {@code bytes}</li>
 * </ul>
 *
 * <h3>Avro/Any → JDBC (для SQL loader)</h3>
 * <ul>
 * <li>{@code int} (Avro date) → {@link java.sql.Date} для PreparedStatement, {@link LocalDate} для BulkCopy</li>
 * <li>{@code long} (Avro timestamp-millis) → {@link java.sql.Timestamp} для PreparedStatement, {@link Instant} для BulkCopy</li>
 * <li>{@link ByteBuffer} (Avro decimal) → {@link BigDecimal}</li>
 * <li>{@link String} (sql_variant JSON) → распаковка через {@link #unpackSqlVariant}</li>
 * <li>{@link SqlVariantValue} → базовый тип через {@link #unpackSqlVariant}</li>
 * </ul>
 *
 * @see SqlVariantValue
 * @see ColumnMetadata
 */
@Component
public class TypeConverter {

    private final ObjectMapper objectMapper;
    private final Conversions.DecimalConversion decimalConversion;

    /**
     * Создать конвертер типов с настройками по умолчанию.
     */
    public TypeConverter() {
        this.objectMapper = new ObjectMapper();
        this.decimalConversion = new Conversions.DecimalConversion();
    }

    /**
     * Создать конвертер типов с заданным ObjectMapper.
     *
     * @param objectMapper Jackson ObjectMapper для JSON-сериализации sql_variant
     */
    public TypeConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.decimalConversion = new Conversions.DecimalConversion();
    }

    // ============ SQL -> Avro (для Kafka Loader с format=AVRO) ============

    /**
     * Конвертирует Java-значение в Avro-совместимый тип согласно Avro schema.
     *
     * <p>Используется в Kafka loader при формировании GenericRecord из EtlRecord.
     *
     * <p>Правила конвертации:
     * <ul>
     * <li>{@link LocalDate} → {@code int} (epoch days) для {@code logicalType: "date"}</li>
     * <li>{@link Instant} → {@code long} (epoch millis) для {@code logicalType: "timestamp-millis"}</li>
     * <li>{@link LocalTime} → {@code long} (microseconds) для {@code logicalType: "time-micros"}</li>
     * <li>{@link BigDecimal} → {@link ByteBuffer} для {@code logicalType: "decimal"}</li>
     * <li>{@link SqlVariantValue} → {@link String} (через {@link #sqlVariantToJson})</li>
     * <li>{@code byte[]} → {@link ByteBuffer} для Avro {@code bytes}</li>
     * <li>Остальные типы → проверка совместимости с физическим типом Avro schema</li>
     * </ul>
     *
     * @param javaValue значение из EtlRecord (может быть LocalDate, Instant, BigDecimal, SqlVariantValue, etc.)
     * @param avroField поле Avro схемы (содержит physical type + logicalType)
     * @param jobId идентификатор ETL job (для диагностики ошибок)
     * @return значение, совместимое с физическим типом Avro schema
     * @throws TypeConversionException если конвертация невозможна
     */
    public Object convertToAvro(Object javaValue, Schema.Field avroField, String jobId) {
        if (javaValue == null) {
            return null;
        }

        Schema schema = unwrapNullableSchema(avroField.schema());
        Schema.Type physicalType = schema.getType();

        // Обработка SqlVariantValue
        if (javaValue instanceof SqlVariantValue) {
            if (physicalType != Schema.Type.STRING) {
                throw new TypeConversionException(
                        String.format("SqlVariantValue field '%s' must be mapped to Avro string type, but got %s",
                                avroField.name(), physicalType),
                        jobId,
                        EtlStage.LOAD
                );
            }
            return sqlVariantToJson((SqlVariantValue) javaValue);
        }

        // Обработка LocalDate → int (date logical type)
        if (javaValue instanceof LocalDate) {
            if (physicalType != Schema.Type.INT) {
                throw new TypeConversionException(
                        String.format("LocalDate field '%s' must be mapped to Avro int type, but got %s",
                                avroField.name(), physicalType),
                        jobId,
                        EtlStage.LOAD
                );
            }
            return (int) ((LocalDate) javaValue).toEpochDay();
        }

        // Обработка java.sql.Date → int (date logical type) - for JDBC sources
        if (javaValue instanceof java.sql.Date) {
            if (physicalType != Schema.Type.INT) {
                throw new TypeConversionException(
                        String.format("java.sql.Date field '%s' must be mapped to Avro int type, but got %s",
                                avroField.name(), physicalType),
                        jobId,
                        EtlStage.LOAD
                );
            }
            return (int) ((java.sql.Date) javaValue).toLocalDate().toEpochDay();
        }

        // Обработка Instant → long (timestamp-millis logical type)
        if (javaValue instanceof Instant) {
            if (physicalType != Schema.Type.LONG) {
                throw new TypeConversionException(
                        String.format("Instant field '%s' must be mapped to Avro long type, but got %s",
                                avroField.name(), physicalType),
                        jobId,
                        EtlStage.LOAD
                );
            }
            return ((Instant) javaValue).toEpochMilli();
        }

        // Обработка java.sql.Timestamp → long (timestamp-millis logical type) - for JDBC sources
        if (javaValue instanceof java.sql.Timestamp) {
            if (physicalType != Schema.Type.LONG) {
                throw new TypeConversionException(
                        String.format("java.sql.Timestamp field '%s' must be mapped to Avro long type, but got %s",
                                avroField.name(), physicalType),
                        jobId,
                        EtlStage.LOAD
                );
            }
            return ((java.sql.Timestamp) javaValue).toInstant().toEpochMilli();
        }

        // Обработка LocalTime → long (time-micros logical type)
        if (javaValue instanceof LocalTime) {
            if (physicalType != Schema.Type.LONG) {
                throw new TypeConversionException(
                        String.format("LocalTime field '%s' must be mapped to Avro long type, but got %s",
                                avroField.name(), physicalType),
                        jobId,
                        EtlStage.LOAD
                );
            }
            return ((LocalTime) javaValue).toNanoOfDay() / 1000; // nanoseconds to microseconds
        }

        // Обработка java.sql.Time → long (time-micros logical type) - for JDBC sources
        if (javaValue instanceof java.sql.Time) {
            if (physicalType != Schema.Type.LONG) {
                throw new TypeConversionException(
                        String.format("java.sql.Time field '%s' must be mapped to Avro long type, but got %s",
                                avroField.name(), physicalType),
                        jobId,
                        EtlStage.LOAD
                );
            }
            return ((java.sql.Time) javaValue).toLocalTime().toNanoOfDay() / 1000; // nanoseconds to microseconds
        }

        // Обработка String → Avro logical types (если драйвер JDBC возвращает строки для дат)
        if (javaValue instanceof String) {
            String strValue = (String) javaValue;

            // Попытка парсинга строки как даты для Avro date логического типа
            if (physicalType == Schema.Type.INT && schema.getLogicalType() instanceof LogicalTypes.Date) {
                try {
                    LocalDate date = LocalDate.parse(strValue);
                    return (int) date.toEpochDay();
                } catch (DateTimeParseException e) {
                    throw new TypeConversionException(
                            String.format("Failed to parse String '%s' as date for field '%s'", strValue, avroField.name()),
                            jobId,
                            EtlStage.LOAD,
                            e
                    );
                }
            }

            // Попытка парсинга строки как timestamp для Avro timestamp-millis
            if (physicalType == Schema.Type.LONG && schema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
                try {
                    // Попробуем несколько форматов
                    Instant instant;
                    if (strValue.contains("T")) {
                        instant = Instant.parse(strValue);
                    } else {
                        // YYYY-MM-DD HH:mm:ss формат
                        LocalDateTime ldt = LocalDateTime.parse(strValue, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                        instant = ldt.atZone(ZoneId.systemDefault()).toInstant();
                    }
                    return instant.toEpochMilli();
                } catch (DateTimeParseException e) {
                    throw new TypeConversionException(
                            String.format("Failed to parse String '%s' as timestamp for field '%s'", strValue, avroField.name()),
                            jobId,
                            EtlStage.LOAD,
                            e
                    );
                }
            }

            // Для остальных String типов - оставить как есть
            if (physicalType == Schema.Type.STRING) {
                return strValue;
            }
        }

        // Обработка OffsetDateTime → string
        if (javaValue instanceof OffsetDateTime) {
            if (physicalType != Schema.Type.STRING) {
                throw new TypeConversionException(
                        String.format("OffsetDateTime field '%s' must be mapped to Avro string type, but got %s",
                                avroField.name(), physicalType),
                        jobId,
                        EtlStage.LOAD
                );
            }
            return ((OffsetDateTime) javaValue).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }

        // Обработка BigDecimal → ByteBuffer (decimal logical type)
        if (javaValue instanceof BigDecimal) {
            if (physicalType != Schema.Type.BYTES) {
                throw new TypeConversionException(
                        String.format("BigDecimal field '%s' must be mapped to Avro bytes type, but got %s",
                                avroField.name(), physicalType),
                        jobId,
                        EtlStage.LOAD
                );
            }

            // Проверка, что есть decimal logical type
            if (schema.getLogicalType() == null || !(schema.getLogicalType() instanceof LogicalTypes.Decimal)) {
                throw new TypeConversionException(
                        String.format("BigDecimal field '%s' requires Avro decimal logicalType",
                                avroField.name()),
                        jobId,
                        EtlStage.LOAD
                );
            }

            LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
            return decimalConversion.toBytes((BigDecimal) javaValue, schema, decimalType);
        }

        // Обработка byte[] → ByteBuffer
        if (javaValue instanceof byte[]) {
            if (physicalType != Schema.Type.BYTES) {
                throw new TypeConversionException(
                        String.format("byte[] field '%s' must be mapped to Avro bytes type, but got %s",
                                avroField.name(), physicalType),
                        jobId,
                        EtlStage.LOAD
                );
            }
            return ByteBuffer.wrap((byte[]) javaValue);
        }

        // Обработка GenericRecord (должно быть ошибкой - GenericRecord должен быть развернут в transformer)
        if (javaValue instanceof GenericRecord) {
            throw new TypeConversionException(
                    String.format("GenericRecord found in field '%s' - should be flattened in AvroToRecordTransformer before SQL load",
                            avroField.name()),
                    jobId,
                    EtlStage.LOAD
            );
        }

        // Остальные типы должны быть совместимы с Avro schema (String, Integer, Long, Boolean, Float, Double)
        if (!isCompatibleWithAvroType(javaValue, physicalType)) {
            throw new TypeConversionException(
                    String.format("Cannot convert %s to Avro %s for field '%s': value=%s",
                            javaValue.getClass().getSimpleName(), physicalType, avroField.name(), javaValue),
                    jobId,
                    EtlStage.LOAD
            );
        }

        return javaValue;
    }

    // ============ Avro/Any -> JDBC (для SQL Loader) ============

    /**
     * Конвертирует значение в JDBC-совместимый тип для PreparedStatement/BulkCopy.
     *
     * <p>Используется в SQL loader перед вставкой данных через JDBC или BulkCopy.
     *
     * <p>Правила конвертации:
     * <ul>
     * <li>{@code int} (Avro date) → {@link java.sql.Date}</li>
     * <li>{@code long} (Avro timestamp-millis) → {@link java.sql.Timestamp}</li>
     * <li>{@link ByteBuffer} (Avro decimal) → {@link BigDecimal}</li>
     * <li>{@link String} (если распознан JSON sql_variant) → распаковка через {@link #unpackSqlVariant}</li>
     * <li>{@link SqlVariantValue} → базовый тип через {@link #unpackSqlVariant}</li>
     * <li>{@link LocalDate} → {@link java.sql.Date}</li>
     * <li>{@link Instant} → {@link java.sql.Timestamp}</li>
     * <li>{@link LocalTime} → {@link java.sql.Time}</li>
     * <li>{@link OffsetDateTime} → {@link String} (ISO format)</li>
     * <li>{@link ByteBuffer} → {@code byte[]}</li>
     * </ul>
     *
     * @param value значение из EtlRecord (может быть результатом convertToAvro или нативный Java-тип)
     * @param targetMetadata метаданные целевой SQL колонки (может быть null если недоступно)
     * @param jobId идентификатор ETL job (для диагностики ошибок)
     * @return значение, готовое к вставке через JDBC
     * @throws TypeConversionException если конвертация невозможна
     */
    public Object convertToJdbc(Object value, ColumnMetadata targetMetadata, String jobId) {
        if (value == null) {
            return null;
        }

        // Обработка SqlVariantValue - распаковка в базовый тип
        if (value instanceof SqlVariantValue) {
            return unpackSqlVariant((SqlVariantValue) value, jobId);
        }

        // Обработка String - может быть JSON sql_variant
        if (value instanceof String && isSqlVariantJson((String) value)) {
            SqlVariantValue variant = sqlVariantFromJson((String) value, jobId);
            return unpackSqlVariant(variant, jobId);
        }

        // ========== Обработка Avro logical types ==========

        // Avro date (int epoch days) → LocalDate
        if (value instanceof Integer && targetMetadata != null
                && "date".equalsIgnoreCase(targetMetadata.getTypeName())) {
            return LocalDate.ofEpochDay((Integer) value);
        }

        // Avro timestamp-millis (long) → Instant
        if (value instanceof Long && targetMetadata != null) {
            String typeName = targetMetadata.getTypeName().toLowerCase();
            if (typeName.equals("datetime2") || typeName.equals("datetime") || typeName.equals("smalldatetime")) {
                return Instant.ofEpochMilli((Long) value);
            }
        }

        // Avro decimal (ByteBuffer) → BigDecimal
        if (value instanceof ByteBuffer && targetMetadata != null && isDecimalType(targetMetadata)) {
            ByteBuffer buffer = ((ByteBuffer) value).duplicate(); // duplicate to avoid side effects

            // Avro decimal encoding: unscaled value in big-endian byte order
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);

            BigInteger unscaledValue = new BigInteger(bytes);
            int scale = targetMetadata.getScale();

            return new BigDecimal(unscaledValue, scale);
        }

        // ========== Обработка canonical Java types ==========

        // Обработка LocalDate → java.sql.Date
        if (value instanceof LocalDate) {
            return Date.valueOf((LocalDate) value);
        }

        // Обработка Instant → java.sql.Timestamp
        if (value instanceof Instant) {
            return Timestamp.from((Instant) value);
        }

        // Обработка LocalTime → java.sql.Time
        if (value instanceof LocalTime) {
            return Time.valueOf((LocalTime) value);
        }

        // Обработка OffsetDateTime → String (SQL Server DATETIMEOFFSET принимает строки в ISO формате)
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }

        // Обработка ByteBuffer → byte[]
        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) value;

            // Может быть Avro decimal - попробуем распаковать
            if (targetMetadata != null && isDecimalType(targetMetadata)) {
                // Не можем распаковать без schema, вернем как есть
                // Loader должен будет обработать это отдельно
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                return bytes;
            }

            // Обычные бинарные данные
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }

        // Все остальные типы возвращаем как есть (String, Integer, Long, Boolean, Float, Double, BigDecimal, byte[])
        return value;
    }

    // ============ sql_variant специфичные методы ============

    /**
     * Создает SqlVariantValue из JDBC значения и SQL_VARIANT_PROPERTY метаданных.
     *
     * <p>Используется в JDBC extractor при чтении sql_variant колонок.
     *
     * <p>Формирует {@code sqlType} в формате SQL Server:
     * <ul>
     * <li>{@code "int"}, {@code "bigint"}, {@code "bit"}</li>
     * <li>{@code "decimal(18,2)"}, {@code "numeric(10,4)"}</li>
     * <li>{@code "datetime2(7)"}, {@code "datetimeoffset(7)"}</li>
     * <li>{@code "varchar(50)"}, {@code "nvarchar(100)"}, {@code "nvarchar(max)"}</li>
     * <li>{@code "varbinary(100)"}, {@code "varbinary(max)"}</li>
     * </ul>
     *
     * @param jdbcValue значение из ResultSet.getObject() для sql_variant колонки
     * @param baseType значение SQL_VARIANT_PROPERTY(col, 'BaseType')
     * @param precision значение SQL_VARIANT_PROPERTY(col, 'Precision') или null
     * @param scale значение SQL_VARIANT_PROPERTY(col, 'Scale') или null
     * @param maxLength значение SQL_VARIANT_PROPERTY(col, 'MaxLength') или null
     * @param jobId идентификатор ETL job (для диагностики ошибок)
     * @return SqlVariantValue с заполненным sqlType в формате SQL Server
     * @throws TypeConversionException если базовый тип не поддерживается
     */
    public SqlVariantValue createSqlVariant(
            Object jdbcValue,
            String baseType,
            Integer precision,
            Integer scale,
            Integer maxLength,
            String jobId
    ) {
        if (jdbcValue == null) {
            // sql_variant может содержать NULL
            return new SqlVariantValue(baseType != null ? baseType : "null", null, "plain");
        }

        if (baseType == null) {
            throw new TypeConversionException(
                    "SQL_VARIANT_PROPERTY('BaseType') returned null for non-null sql_variant value",
                    jobId,
                    EtlStage.EXTRACT
            );
        }

        String sqlType = formatSqlType(baseType.toLowerCase(), precision, scale, maxLength);
        String encoding = "plain";
        String valueStr;

        // Обработка бинарных типов
        if (jdbcValue instanceof byte[]) {
            encoding = "base64";
            valueStr = Base64.getEncoder().encodeToString((byte[]) jdbcValue);
        }
        // Обработка дат и времени
        else if (jdbcValue instanceof Timestamp) {
            valueStr = ((Timestamp) jdbcValue).toInstant().toString();
        } else if (jdbcValue instanceof Date) {
            valueStr = ((Date) jdbcValue).toLocalDate().toString();
        } else if (jdbcValue instanceof Time) {
            valueStr = ((Time) jdbcValue).toLocalTime().toString();
        }
        // Все остальные типы - toString
        else {
            valueStr = jdbcValue.toString();
        }

        return new SqlVariantValue(sqlType, valueStr, encoding);
    }

    /**
     * Распаковывает SqlVariantValue в базовый JDBC-совместимый Java-тип.
     *
     * <p>Используется в SQL loader перед вставкой sql_variant значений.
     *
     * <p>Возвращаемые типы (в зависимости от baseType):
     * <ul>
     * <li>{@link Integer} для {@code int, smallint, tinyint}</li>
     * <li>{@link Long} для {@code bigint}</li>
     * <li>{@link BigDecimal} для {@code decimal, numeric, money, smallmoney}</li>
     * <li>{@link Boolean} для {@code bit}</li>
     * <li>{@link Float} для {@code real}</li>
     * <li>{@link Double} для {@code float}</li>
     * <li>{@link String} для {@code varchar, nvarchar, char, nchar, xml}</li>
     * <li>{@code byte[]} для {@code varbinary, binary, image}</li>
     * <li>{@link Timestamp} для {@code datetime, datetime2, smalldatetime}</li>
     * <li>{@link Date} для {@code date}</li>
     * <li>{@link Time} для {@code time}</li>
     * </ul>
     *
     * @param variant контейнер sql_variant
     * @param jobId идентификатор ETL job (для диагностики ошибок)
     * @return базовый Java-тип
     * @throws TypeConversionException если базовый тип не поддерживается или значение невалидно
     */
    public Object unpackSqlVariant(SqlVariantValue variant, String jobId) {
        if (variant.getValue() == null) {
            return null;
        }

        String sqlType = variant.getSqlType().toLowerCase();
        String value = variant.getValue();
        String encoding = variant.getEncoding();

        try {
            // Извлечь базовый тип без параметров
            String baseType = extractBaseType(sqlType);

            switch (baseType) {
                case "int":
                case "smallint":
                case "tinyint":
                    return Integer.parseInt(value);

                case "bigint":
                    return Long.parseLong(value);

                case "bit":
                    return Boolean.parseBoolean(value) || "1".equals(value);

                case "real":
                    return Float.parseFloat(value);

                case "float":
                    return Double.parseDouble(value);

                case "decimal":
                case "numeric":
                case "money":
                case "smallmoney":
                    return new BigDecimal(value);

                case "varchar":
                case "nvarchar":
                case "char":
                case "nchar":
                case "text":
                case "ntext":
                case "xml":
                    return value;

                case "varbinary":
                case "binary":
                case "image":
                case "rowversion":
                case "timestamp":
                    if ("base64".equals(encoding)) {
                        return Base64.getDecoder().decode(value);
                    } else if ("hex".equals(encoding)) {
                        return hexToBytes(value);
                    } else {
                        throw new TypeConversionException(
                                String.format("Unsupported encoding '%s' for binary sql_variant type '%s'",
                                        encoding, sqlType),
                                jobId,
                                EtlStage.LOAD
                        );
                    }

                case "date":
                    return Date.valueOf(LocalDate.parse(value));

                case "time":
                    return Time.valueOf(LocalTime.parse(value));

                case "datetime":
                case "datetime2":
                case "smalldatetime":
                    return Timestamp.from(Instant.parse(value));

                case "datetimeoffset":
                    // DATETIMEOFFSET требует строкового формата для JDBC
                    return value;

                case "uniqueidentifier":
                    return value; // UUID как строка

                case "geography":
                case "geometry":
                case "hierarchyid":
                    return value; // Пространственные типы как строки

                default:
                    throw new TypeConversionException(
                            String.format("Unsupported sql_variant base type '%s'", sqlType),
                            jobId,
                            EtlStage.LOAD
                    );
            }
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new TypeConversionException(
                    String.format("Failed to parse sql_variant value for type '%s': value='%s'",
                            sqlType, value),
                    jobId,
                    EtlStage.LOAD,
                    e
            );
        }
    }

    /**
     * Сериализует SqlVariantValue в JSON-строку для хранения в Avro.
     *
     * <p>Формат JSON:
     * <pre>
     * {"v":1,"t":"decimal(18,2)","val":"12.30","enc":"plain"}
     * </pre>
     *
     * <p>Поля:
     * <ul>
     * <li>{@code v} - версия формата (1)</li>
     * <li>{@code t} - sqlType (точное имя SQL Server типа с параметрами)</li>
     * <li>{@code val} - значение (строковое представление)</li>
     * <li>{@code enc} - encoding (plain, base64, hex)</li>
     * </ul>
     *
     * @param variant контейнер sql_variant
     * @return JSON-строка
     * @throws TypeConversionException если сериализация не удалась
     */
    public String sqlVariantToJson(SqlVariantValue variant) {
        try {
            ObjectNode json = objectMapper.createObjectNode();
            json.put("v", 1);
            json.put("t", variant.getSqlType());
            json.put("val", variant.getValue());
            json.put("enc", variant.getEncoding());
            return objectMapper.writeValueAsString(json);
        } catch (JsonProcessingException e) {
            throw new TypeConversionException(
                    String.format("Failed to serialize SqlVariantValue to JSON: sqlType='%s'",
                            variant.getSqlType()),
                    "unknown", // jobId not available in this context
                    EtlStage.LOAD,
                    e
            );
        }
    }

    /**
     * Десериализует JSON-строку обратно в SqlVariantValue.
     *
     * <p>Ожидает формат:
     * <pre>
     * {"v":1,"t":"decimal(18,2)","val":"12.30","enc":"plain"}
     * </pre>
     *
     * @param json JSON-строка из Avro
     * @param jobId идентификатор ETL job (для диагностики ошибок)
     * @return SqlVariantValue
     * @throws TypeConversionException если JSON невалиден или версия не поддерживается
     */
    public SqlVariantValue sqlVariantFromJson(String json, String jobId) {
        try {
            JsonNode node = objectMapper.readTree(json);

            // Проверка версии
            if (!node.has("v") || node.get("v").asInt() != 1) {
                throw new TypeConversionException(
                        String.format("Unsupported SqlVariantValue JSON version: expected 1, got %s",
                                node.has("v") ? node.get("v").asInt() : "missing"),
                        jobId,
                        EtlStage.EXTRACT
                );
            }

            String sqlType = node.get("t").asText();
            String value = node.has("val") && !node.get("val").isNull() ? node.get("val").asText() : null;
            String encoding = node.get("enc").asText();

            return new SqlVariantValue(sqlType, value, encoding);
        } catch (JsonProcessingException e) {
            throw new TypeConversionException(
                    String.format("Failed to deserialize SqlVariantValue from JSON: %s", json),
                    jobId,
                    EtlStage.EXTRACT,
                    e
            );
        }
    }

    // ============ Вспомогательные методы ============

    /**
     * Unwrap nullable union schema to get actual type.
     */
    private Schema unwrapNullableSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema type : schema.getTypes()) {
                if (type.getType() != Schema.Type.NULL) {
                    return type;
                }
            }
        }
        return schema;
    }

    /**
     * Проверка совместимости Java-типа с Avro физическим типом.
     */
    private boolean isCompatibleWithAvroType(Object value, Schema.Type avroType) {
        switch (avroType) {
            case BOOLEAN:
                return value instanceof Boolean;
            case INT:
                return value instanceof Integer;
            case LONG:
                return value instanceof Long;
            case FLOAT:
                return value instanceof Float;
            case DOUBLE:
                return value instanceof Double;
            case STRING:
                return value instanceof String || value instanceof CharSequence;
            case BYTES:
                return value instanceof ByteBuffer || value instanceof byte[];
            default:
                return false;
        }
    }

    /**
     * Проверка, является ли строка JSON sql_variant.
     */
    private boolean isSqlVariantJson(String value) {
        if (value == null || !value.startsWith("{")) {
            return false;
        }
        try {
            JsonNode node = objectMapper.readTree(value);
            return node.has("v") && node.has("t") && node.has("val") && node.has("enc");
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    /**
     * Проверка, является ли тип DECIMAL-подобным.
     */
    private boolean isDecimalType(ColumnMetadata metadata) {
        String typeName = metadata.getTypeName().toLowerCase();
        return typeName.equals("decimal") || typeName.equals("numeric") ||
                typeName.equals("money") || typeName.equals("smallmoney");
    }

    /**
     * Форматирует SQL тип с параметрами.
     *
     * <p>Примеры:
     * <ul>
     * <li>int → "int"</li>
     * <li>decimal(18,2) → "decimal(18,2)"</li>
     * <li>varchar(50) → "varchar(50)"</li>
     * <li>varbinary(max) → "varbinary(max)" (maxLength = -1)</li>
     * </ul>
     */
    private String formatSqlType(String baseType, Integer precision, Integer scale, Integer maxLength) {
        switch (baseType) {
            case "decimal":
            case "numeric":
                return String.format("%s(%d,%d)", baseType,
                        precision != null ? precision : 18,
                        scale != null ? scale : 0);

            case "datetime2":
            case "datetimeoffset":
            case "time":
                return String.format("%s(%d)", baseType, scale != null ? scale : 7);

            case "varchar":
            case "nvarchar":
            case "char":
            case "nchar":
            case "varbinary":
            case "binary":
                if (maxLength != null && maxLength == -1) {
                    return String.format("%s(max)", baseType);
                } else if (maxLength != null) {
                    return String.format("%s(%d)", baseType, maxLength);
                }
                return baseType;

            default:
                // Типы без параметров
                return baseType;
        }
    }

    /**
     * Извлекает базовый тип без параметров из sqlType.
     *
     * <p>Примеры:
     * <ul>
     * <li>"decimal(18,2)" → "decimal"</li>
     * <li>"varchar(50)" → "varchar"</li>
     * <li>"int" → "int"</li>
     * </ul>
     */
    private String extractBaseType(String sqlType) {
        int parenIndex = sqlType.indexOf('(');
        return parenIndex > 0 ? sqlType.substring(0, parenIndex) : sqlType;
    }

    /**
     * Конвертирует hex-строку в byte array.
     */
    private byte[] hexToBytes(String hex) {
        // Убрать префикс 0x если есть
        if (hex.startsWith("0x") || hex.startsWith("0X")) {
            hex = hex.substring(2);
        }

        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}
