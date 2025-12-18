# План реализации конвертации типов SQL Server <-> Avro

Документ содержит пошаговый план реализации требований из `doc/type-conversion-requirements.md`.

**Связанные документы:**
- `doc/type-conversion-requirements.md` - требования
- `doc/type-conversion.md` - матрица типов

---

## Принципы реализации

1. **Поэтапность:** Каждый этап можно завершить, протестировать и закоммитить отдельно
2. **Обратная совместимость на переходный период:** Breaking changes вводятся постепенно
3. **Тесты сразу:** Unit-тесты пишутся вместе с кодом, интеграционные - в конце этапа
4. **Документация:** JavaDoc обязательна для новых классов и публичных API

---

## Этап 0: Подготовка (оценка: 1-2 часа)

### 0.1. Создание issue/task tracking
- [ ] Создать задачу в системе отслеживания (если используется)
- [ ] Создать feature branch: `feature/type-conversion`

### 0.2. Добавление зависимостей
- [x] JSON библиотека: Jackson уже есть в проекте ✅
- [x] Avro зависимости: уже есть (org.apache.avro:avro:1.11.3) ✅
- [x] JSqlParser для парсинга SQL запросов ✅ (добавлено в pom.xml)

### 0.3. Проверка текущих тестов
- [ ] Запустить все существующие тесты: `mvn clean test`
- [ ] Убедиться что все проходят (baseline)

---

## Этап 1: Создание базовых классов модели (оценка: 4-6 часов)

### 1.1. Создать `ColumnMetadata`
**Файл:** `src/main/java/ru/pospelov/etl/engine/model/ColumnMetadata.java`

```java
package ru.pospelov.etl.engine.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * Метаданные одной колонки из ResultSetMetaData.
 * Используется для передачи типовой информации от JDBC extractor до loader'ов.
 */
@AllArgsConstructor
@Getter
@ToString
public class ColumnMetadata {
    private final String columnName;      // имя колонки
    private final int jdbcType;           // java.sql.Types.*
    private final String typeName;        // "NVARCHAR", "DATETIME2", "sql_variant", etc.
    private final int precision;          // 0 если не применимо
    private final int scale;              // 0 если не применимо
    private final boolean nullable;
}
```

**Тесты:** `src/test/java/ru/pospelov/etl/engine/model/ColumnMetadataTest.java`
- [ ] Создание объекта
- [ ] Getter'ы работают корректно
- [ ] toString() содержит все поля

### 1.2. Создать `EtlBatch`
**Файл:** `src/main/java/ru/pospelov/etl/engine/model/EtlBatch.java`

```java
package ru.pospelov.etl.engine.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Контейнер для batch записей с метаданными колонок.
 *
 * Используется для передачи данных между extractor -> transformer -> loader.
 * JDBC extractor заполняет columnMetadata, Kafka extractor оставляет null.
 */
@AllArgsConstructor
@Getter
@ToString(exclude = "records") // Не выводить records в toString - может быть много
public class EtlBatch {
    private final Collection<EtlRecord> records;

    /**
     * Метаданные колонок источника данных.
     * Ключ - имя колонки (как в SQL или как в EtlRecord.fields).
     * Может быть null для non-JDBC источников (например Kafka).
     */
    private final Map<String, ColumnMetadata> columnMetadata;

    /**
     * Получить неизменяемую view метаданных.
     */
    public Map<String, ColumnMetadata> getColumnMetadata() {
        return columnMetadata != null
            ? Collections.unmodifiableMap(columnMetadata)
            : null;
    }
}
```

**Тесты:** `src/test/java/ru/pospelov/etl/engine/model/EtlBatchTest.java`
- [ ] Создание с metadata и без
- [ ] Getter'ы работают
- [ ] getColumnMetadata() возвращает unmodifiable map

### 1.3. Создать `SqlVariantValue`
**Файл:** `src/main/java/ru/pospelov/etl/engine/model/SqlVariantValue.java`

```java
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
 * <h2>Жизненный цикл в pipeline</h2>
 * <ol>
 * <li>JDBC Extractor читает sql_variant колонку + SQL_VARIANT_PROPERTY метаданные,
 *     создает SqlVariantValue через TypeConverter.createSqlVariant()</li>
 * <li>Kafka Loader (format=AVRO) сериализует в JSON-строку через TypeConverter.sqlVariantToJson():
 *     {"v":1,"t":"decimal(18,2)","val":"12.30","enc":"plain"}</li>
 * <li>Kafka Extractor десериализует JSON обратно в SqlVariantValue через
 *     TypeConverter.sqlVariantFromJson()</li>
 * <li>SQL Loader распаковывает в базовый Java-тип через TypeConverter.unpackSqlVariant()
 *     для JDBC вставки (Integer/BigDecimal/byte[]/etc.)</li>
 * </ol>
 *
 * <h2>Формат полей</h2>
 * <ul>
 * <li><b>sqlType</b>: точная строка типа как в SQL Server. Примеры:
 *     "int", "decimal(18,2)", "datetime2(7)", "varchar(50)", "varbinary(max)"</li>
 * <li><b>value</b>: каноническое строковое представление значения.
 *     Для бинарных данных используется encoding base64/hex</li>
 * <li><b>encoding</b>: "plain" (по умолчанию для всех типов кроме бинарных),
 *     "base64" или "hex" (для varbinary)</li>
 * </ul>
 *
 * <h2>JSON формат для Avro</h2>
 * <pre>
 * {"v":1,"t":"decimal(18,2)","val":"12.30","enc":"plain"}
 *
 * Поля JSON:
 * - v: версия формата (обязательна для будущей совместимости)
 * - t: sqlType
 * - val: value
 * - enc: encoding
 * </pre>
 *
 * <h2>Ограничения при bulk copy</h2>
 * При вставке через bulk copy в SQL Server, строковые базовые типы автоматически
 * конвертируются: varchar → nvarchar, char → nchar. Это ограничение bulk copy API,
 * точное восстановление требует PreparedStatement с CAST (не используется из-за
 * производительности). См. doc/type-conversion-requirements.md раздел 4.6.
 *
 * @see ru.pospelov.etl.engine.conversion.TypeConverter#createSqlVariant
 * @see ru.pospelov.etl.engine.conversion.TypeConverter#sqlVariantToJson
 * @see ru.pospelov.etl.engine.conversion.TypeConverter#unpackSqlVariant
 */
@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
public class SqlVariantValue {
    /**
     * Точный базовый тип как в SQL Server.
     * Примеры: "int", "decimal(18,2)", "datetime2(7)", "varbinary(max)"
     */
    private final String sqlType;

    /**
     * Каноническое строковое представление значения.
     * Для бинарных данных используется кодирование согласно полю encoding.
     */
    private final String value;

    /**
     * Кодирование значения: "plain", "base64", "hex".
     * По умолчанию "plain" для всех типов кроме бинарных.
     */
    private final String encoding;
}
```

**Тесты:** `src/test/java/ru/pospelov/etl/engine/model/SqlVariantValueTest.java`
- [ ] Создание для разных типов (int, decimal, datetime2, varchar, varbinary)
- [ ] equals/hashCode работают корректно
- [ ] toString содержит все поля

### 1.4. Создать exception для конвертации
**Файл:** `src/main/java/ru/pospelov/etl/engine/exception/TypeConversionException.java`

```java
package ru.pospelov.etl.engine.exception;

/**
 * Исключение при ошибках конвертации типов между Java, Avro и JDBC.
 */
public class TypeConversionException extends EtlException {

    public TypeConversionException(String message, String jobId) {
        super(message, jobId, null, null, EtlErrorSeverity.CRITICAL);
    }

    public TypeConversionException(String message, String jobId, Throwable cause) {
        super(message, jobId, null, null, EtlErrorSeverity.CRITICAL, cause);
    }

    public TypeConversionException(String message, String jobId, String partition, Long offset, Throwable cause) {
        super(message, jobId, partition, offset, EtlErrorSeverity.CRITICAL, cause);
    }
}
```

**Тесты:** Простые unit-тесты на создание и getMessage()

---

## Этап 2: Создание TypeConverter (оценка: 8-12 часов)

### 2.1. Создать интерфейс/класс TypeConverter
**Файл:** `src/main/java/ru/pospelov/etl/engine/conversion/TypeConverter.java`

**Зависимости:**
- Jackson `ObjectMapper` для JSON сериализации (инъекция через конструктор или @Autowired)
- Avro `Schema` для анализа типов

Реализовать все методы согласно контракту из `doc/type-conversion-requirements.md` раздел 5.1:
- `Object convertToAvro(Object javaValue, Schema.Field avroField)`
- `Object convertToJdbc(Object value, ColumnMetadata targetMetadata)`
- `SqlVariantValue createSqlVariant(Object jdbcValue, String baseType, Integer precision, Integer scale, Integer maxLength)`
- `Object unpackSqlVariant(SqlVariantValue variant)`
- `String sqlVariantToJson(SqlVariantValue variant)`
- `SqlVariantValue sqlVariantFromJson(String json)`

**Ключевые моменты реализации:**

1. **convertToAvro:**
   - LocalDate → Integer (epoch days)
   - Instant → Long (epoch millis)
   - LocalTime → Long (microseconds)
   - BigDecimal → ByteBuffer (Avro decimal encoding)
   - SqlVariantValue → String (через sqlVariantToJson)
   - byte[] → ByteBuffer
   - Проверка совместимости с физическим типом Avro schema

2. **convertToJdbc:**
   - Integer (Avro date) → LocalDate для bulk copy
   - Long (Avro timestamp) → Instant для bulk copy
   - ByteBuffer (Avro decimal) → BigDecimal
   - String (если JSON sql_variant) → SqlVariantValue → unpack
   - GenericRecord → TypeConversionException (должен быть развернут ранее)

3. **createSqlVariant:**
   - Собрать sqlType из baseType, precision, scale, maxLength
   - Примеры: "int", "decimal(18,2)", "datetime2(7)", "varchar(50)"
   - Определить encoding (plain для большинства, base64 для binary)
   - Конвертировать value в строку

4. **unpackSqlVariant:**
   - Парсить sqlType (regex или switch по baseType)
   - Конвертировать value обратно в Java-тип:
     - "int" → Integer.parseInt(value)
     - "decimal(p,s)" → new BigDecimal(value)
     - "datetime2" → parse в Instant
     - "varbinary" → Base64.decode(value)
     - и т.д.

5. **sqlVariantToJson/sqlVariantFromJson:**
   - Использовать Jackson ObjectMapper
   - Формат: `{"v":1,"t":"...","val":"...","enc":"..."}`
   - Валидация версии при десериализации

**Тесты:** `src/test/java/ru/pospelov/etl/engine/conversion/TypeConverterTest.java`

Обязательные тест-кейсы (из requirements 7.1):
- [ ] convertToAvro: LocalDate → int (epoch days), включая 1970-01-01
- [ ] convertToAvro: Instant → long (epoch millis) с UTC
- [ ] convertToAvro: BigDecimal → ByteBuffer с precision/scale
- [ ] convertToAvro: SqlVariantValue → JSON string
- [ ] convertToAvro: byte[] → ByteBuffer
- [ ] convertToAvro: null → null
- [ ] convertToJdbc: int (Avro date) → LocalDate
- [ ] convertToJdbc: long (Avro timestamp) → Instant
- [ ] convertToJdbc: ByteBuffer (decimal) → BigDecimal
- [ ] convertToJdbc: String (JSON variant) → SqlVariantValue
- [ ] createSqlVariant: разные базовые типы (int, decimal(18,2), datetime2(7), varchar(50), varbinary)
- [ ] unpackSqlVariant: разные базовые типы обратно в Java
- [ ] sqlVariantToJson/FromJson: round-trip для всех типов
- [ ] sqlVariantFromJson: невалидный JSON → TypeConversionException
- [ ] sqlVariantFromJson: неподдерживаемая версия → TypeConversionException
- [ ] sql_variant с разными базовыми типами в разных записях
- [ ] sql_variant с бинарными данными (base64 encoding)

---

## Этап 3: Обновление EtlBulkRecord.mapJavaToSqlType (оценка: 2-3 часа)

### 3.1. Добавить поддержку Java time-типов
**Файл:** `src/main/java/ru/pospelov/etl/engine/model/EtlBulkRecord.java`

Изменить метод `mapJavaToSqlType`:

```java
private int mapJavaToSqlType(Object value) {
    if (value == null) return Types.VARCHAR;

    // Добавить новые типы
    if (value instanceof LocalDate) return Types.DATE;
    if (value instanceof Instant) return Types.TIMESTAMP;
    if (value instanceof LocalTime) return Types.TIME;
    if (value instanceof OffsetDateTime) return Types.TIMESTAMP_WITH_TIMEZONE;

    // Existing switch for other types
    return switch (value.getClass().getSimpleName()) {
        case "Integer" -> Types.INTEGER;
        case "Long" -> Types.BIGINT;
        case "Double" -> Types.DOUBLE;
        case "Float" -> Types.FLOAT;
        case "BigDecimal" -> Types.DECIMAL;
        case "Boolean" -> Types.BOOLEAN;
        case "LocalDateTime", "Timestamp" -> Types.TIMESTAMP;
        default -> Types.VARCHAR;
    };
}
```

**Тесты:** `src/test/java/ru/pospelov/etl/engine/model/EtlBulkRecordTest.java`
- [ ] LocalDate → Types.DATE
- [ ] Instant → Types.TIMESTAMP
- [ ] LocalTime → Types.TIME
- [ ] OffsetDateTime → Types.TIMESTAMP_WITH_TIMEZONE

---

## Этап 4: Изменение сигнатур (Breaking Changes) (оценка: 6-8 часов)

### 4.1. Обновить JDBC Extractor
**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcStreamingResultSetExtractor.java`

**Изменения:**
1. Изменить сигнатуру конструктора: `Consumer<Collection<EtlRecord>>` → `Consumer<EtlBatch>`
2. Добавить инъекцию `TypeConverter` (или создать внутри)
3. Добавить логику для sql_variant колонок:
   - Собрать `Map<String, ColumnMetadata>` из `ResultSetMetaData`
   - Определить sql_variant колонки по `typeName == "sql_variant"`
   - При чтении строки: для sql_variant колонок читать `__variant_*` properties и создавать `SqlVariantValue`
4. Создавать `EtlBatch(records, columnMetadata)` вместо просто `Collection<EtlRecord>`
5. Переименовать `"key"` → `"__kafka_key"` если используется

**Псевдокод:**
```java
@Override
public Void extractData(ResultSet rs) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();

    // 1. Собрать column metadata
    Map<String, ColumnMetadata> columnMetadata = buildColumnMetadata(md);

    // 2. Определить sql_variant колонки
    Set<String> variantColumns = columnMetadata.entrySet().stream()
        .filter(e -> "sql_variant".equals(e.getValue().getTypeName()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());

    List<EtlRecord> currentBatch = new ArrayList<>(batchSize);

    while (rs.next()) {
        EtlRecord record = new EtlRecord(Instant.now(), sourcePartition, rs.getRow());

        for (int i = 1; i <= md.getColumnCount(); i++) {
            String colName = md.getColumnLabel(i);

            // Пропустить __variant_* вспомогательные колонки
            if (colName.startsWith("__variant_")) {
                continue;
            }

            if (variantColumns.contains(colName)) {
                // Читаем sql_variant + properties
                Object jdbcValue = rs.getObject(colName);
                String baseType = rs.getString("__variant_" + colName + "_basetype");
                Integer precision = getIntOrNull(rs, "__variant_" + colName + "_precision");
                Integer scale = getIntOrNull(rs, "__variant_" + colName + "_scale");
                Integer maxLength = getIntOrNull(rs, "__variant_" + colName + "_maxlength");

                SqlVariantValue variant = typeConverter.createSqlVariant(
                    jdbcValue, baseType, precision, scale, maxLength
                );
                record.put(colName, variant);
            } else {
                // Обычная колонка
                Object value = rs.getObject(colName);
                record.put(colName, value);
            }
        }

        // Добавить __kafka_key если нужно
        if (!keyColumn.isEmpty()) {
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

private Map<String, ColumnMetadata> buildColumnMetadata(ResultSetMetaData md) throws SQLException {
    Map<String, ColumnMetadata> metadata = new LinkedHashMap<>();
    for (int i = 1; i <= md.getColumnCount(); i++) {
        String colName = md.getColumnLabel(i);

        // Пропустить __variant_* вспомогательные колонки
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

private Integer getIntOrNull(ResultSet rs, String columnName) throws SQLException {
    int value = rs.getInt(columnName);
    return rs.wasNull() ? null : value;
}
```

### 4.2. Обновить Kafka Extractor
**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/kafka/KafkaPartitionExtractor.java`

**Изменения:**
1. Изменить сигнатуру `Consumer<Collection<EtlRecord>>` → `Consumer<EtlBatch>`
2. Переименовать `"key"` → `"__kafka_key"`, `"value"` → `"__kafka_value"`
3. Создавать `EtlBatch(records, null)` (metadata = null для Kafka источника)

```java
EtlRecord etlRecord = new EtlRecord(...);
if (record.key() != null) {
    etlRecord.put("__kafka_key", record.key());
}
if (record.value() != null) {
    etlRecord.put("__kafka_value", record.value());
}

// ...

if (currentBatch.size() >= streamBatchSize) {
    batchConsumer.accept(new EtlBatch(new ArrayList<>(currentBatch), null));
    currentBatch.clear();
}
```

### 4.3. Обновить Transformer интерфейс и реализации
**Файлы:**
- `src/main/java/ru/pospelov/etl/engine/steps/transformer/Transformer.java`
- `src/main/java/ru/pospelov/etl/engine/steps/transformer/NoopTransformer.java`
- `src/main/java/ru/pospelov/etl/engine/steps/transformer/RecordToAvroTransformer.java`
- `src/main/java/ru/pospelov/etl/engine/steps/transformer/AvroToRecordTransformer.java`

**Изменения:**
1. Изменить сигнатуру: `Collection<EtlRecord> transform(...)` → `EtlBatch transform(EtlBatch batch, ...)`
2. NoopTransformer: просто возвращать batch как есть
3. RecordToAvroTransformer: обновить логику чтения `"__kafka_key"` вместо `"key"`
4. AvroToRecordTransformer: обновить логику записи `"__kafka_value"` вместо `"value"`

### 4.4. Обновить Loader интерфейс и реализации
**Файлы:**
- `src/main/java/ru/pospelov/etl/engine/steps/loader/Loader.java`
- `src/main/java/ru/pospelov/etl/engine/steps/loader/JdbcLoader.java`
- `src/main/java/ru/pospelov/etl/engine/steps/loader/FastSqlServerLoader.java`
- `src/main/java/ru/pospelov/etl/engine/steps/loader/KafkaByPartitionLoader.java`

**Изменения:**
1. Изменить сигнатуру: `void load(..., Collection<EtlRecord> records)` → `void load(..., EtlBatch batch)`
2. Добавить инъекцию TypeConverter во все loader'ы
3. Реализовать логику конвертации (см. Этап 5)

### 4.5. Обновить Pipeline
**Файлы:**
- `src/main/java/ru/pospelov/etl/engine/pipeline/StreamingEtlPipeline.java`
- Любые другие места где используется `Consumer<Collection<EtlRecord>>`

**Изменения:**
1. Обновить типы переменных и сигнатуры методов
2. Проверить что все вызовы передают `EtlBatch`

---

## Этап 5: Реализация конвертации в Loader'ах (оценка: 8-10 часов)

### 5.1. Kafka Loader (format=AVRO) - конвертация SQL -> Avro
**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/loader/KafkaByPartitionLoader.java`

**Изменения:**
1. Добавить получение Avro schema из Schema Registry (если format=AVRO)
2. Для каждого record: собрать GenericRecord через TypeConverter.convertToAvro
3. Обновить чтение `"__kafka_key"` вместо `"key"`

**Псевдокод:**
```java
public void load(KafkaLoaderConfig config, String jobId, EtlBatch batch) {
    if (batch.getRecords().isEmpty()) return;

    KafkaFormat format = config.format();
    String topic = config.topic();

    Producer<String, Object> producer = getProducer(format == KafkaFormat.AVRO);

    if (format == KafkaFormat.AVRO) {
        // Получить schema
        Schema schema = schemaRegistryService.getLatestSchema(config.avroSchemaSubject());

        for (EtlRecord record : batch.getRecords()) {
            // Собрать GenericRecord
            GenericRecord gr = new GenericData.Record(schema);

            for (Schema.Field field : schema.getFields()) {
                Object rawValue = record.get(field.name());
                if (rawValue == null) {
                    gr.put(field.name(), null);
                    continue;
                }

                try {
                    Object avroValue = typeConverter.convertToAvro(rawValue, field);
                    gr.put(field.name(), avroValue);
                } catch (TypeConversionException e) {
                    // Логировать с контекстом
                    log.error("Type mismatch at SQL->Avro: field={} expected={} actual={} value={} job={} partition={} offset={}",
                        field.name(),
                        field.schema(),
                        rawValue.getClass().getName(),
                        truncate(rawValue.toString(), 100),
                        jobId,
                        record.getSourcePartition(),
                        record.getOffset(),
                        e
                    );
                    throw new LoadingException("Type conversion failed for field: " + field.name(), jobId, record, EtlErrorSeverity.CRITICAL, e);
                }
            }

            // Публикация
            String key = (String) record.get("__kafka_key");
            ProducerRecord<String, Object> pr = new ProducerRecord<>(topic, key, gr);
            producer.send(pr);
        }
    } else {
        // STRING format - как было
        for (EtlRecord record : batch.getRecords()) {
            String key = (String) record.get("__kafka_key");
            Object value = record.get("__kafka_value");
            ProducerRecord<String, Object> pr = new ProducerRecord<>(topic, key, value);
            producer.send(pr);
        }
    }

    producer.flush();
}
```

### 5.2. JDBC Loader - конвертация Avro -> SQL
**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/loader/JdbcLoader.java`

**Изменения:**
1. Фильтровать служебные поля `__kafka_*` при формировании SQL
2. Конвертировать значения через TypeConverter.convertToJdbc перед setObject

**Псевдокод:**
```java
public void load(JdbcLoaderConfig config, String jobId, EtlBatch batch) {
    if (batch.getRecords().isEmpty()) return;

    // Получить user columns (без __kafka_*)
    Set<String> userColumns = batch.getRecords().iterator().next().getAll().keySet().stream()
        .filter(k -> !k.startsWith("__kafka_"))
        .filter(k -> !k.startsWith("__variant_"))
        .collect(Collectors.toSet());

    String sql = buildInsertSql(config.table(), userColumns);

    jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
        @Override
        public void setValues(PreparedStatement ps, int i) throws SQLException {
            EtlRecord record = batch.getRecords().get(i);
            int paramIndex = 1;

            for (String columnName : userColumns) {
                Object rawValue = record.get(columnName);

                ColumnMetadata meta = batch.getColumnMetadata() != null
                    ? batch.getColumnMetadata().get(columnName)
                    : null;

                try {
                    Object jdbcValue = typeConverter.convertToJdbc(rawValue, meta);
                    ps.setObject(paramIndex++, jdbcValue);
                } catch (TypeConversionException e) {
                    log.error("Type mismatch at Avro->SQL: field={} actual={} value={} job={} partition={} offset={}",
                        columnName,
                        rawValue != null ? rawValue.getClass().getName() : "null",
                        truncate(String.valueOf(rawValue), 100),
                        jobId,
                        record.getSourcePartition(),
                        record.getOffset(),
                        e
                    );
                    throw e;
                }
            }
        }

        @Override
        public int getBatchSize() {
            return batch.getRecords().size();
        }
    });
}
```

### 5.3. FastSqlServerLoader (Bulk Copy) - конвертация Avro -> SQL
**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/loader/FastSqlServerLoader.java`

**Изменения:**
1. Обновить создание `EtlBulkRecord` с фильтрацией служебных полей
2. В `EtlBulkRecord` добавить конвертацию через TypeConverter перед getRowData()

**Вариант 1: Фильтрация в FastSqlServerLoader**
```java
public void load(FastSqlLoaderConfig config, String jobId, EtlBatch batch) {
    // Фильтровать записи: убрать __kafka_* и __variant_* поля
    Collection<EtlRecord> filteredRecords = batch.getRecords().stream()
        .map(record -> {
            EtlRecord filtered = new EtlRecord(record.getTimestamp(), record.getSourcePartition(), record.getOffset());
            record.getAll().forEach((key, value) -> {
                if (!key.startsWith("__kafka_") && !key.startsWith("__variant_")) {
                    // Конвертировать через TypeConverter
                    ColumnMetadata meta = batch.getColumnMetadata() != null
                        ? batch.getColumnMetadata().get(key)
                        : null;
                    Object jdbcValue = typeConverter.convertToJdbc(value, meta);
                    filtered.put(key, jdbcValue);
                }
            });
            return filtered;
        })
        .collect(Collectors.toList());

    EtlBulkRecord bulkRecord = new EtlBulkRecord(filteredRecords);

    // Остальная логика как было
    sqlBulkCopy.writeToServer(bulkRecord);
}
```

**Вариант 2: Модифицировать EtlBulkRecord конструктор**
Принимать `EtlBatch` вместо `Collection<EtlRecord>` и фильтровать/конвертировать внутри.

---

## Этап 6: Автогенерация SQL для sql_variant (table-based) (оценка: 6-8 часов)

### 6.1. Создать SQL Generator для table-based конфигурации
**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/SqlVariantQueryGenerator.java`

```java
package ru.pospelov.etl.engine.steps.extractor.jdbc;

/**
 * Генератор SQL запросов с автоматическим добавлением SQL_VARIANT_PROPERTY
 * для table-based конфигураций JDBC extractor.
 */
public class SqlVariantQueryGenerator {

    /**
     * Генерирует SELECT запрос для таблицы с автоматическим добавлением
     * SQL_VARIANT_PROPERTY колонок для всех sql_variant полей.
     *
     * @param tableName имя таблицы (может включать схему: dbo.orders)
     * @param dataSource DataSource для выполнения INFORMATION_SCHEMA запросов
     * @return сгенерированный SQL запрос
     */
    public String generateSelectQuery(String tableName, DataSource dataSource) {
        // 1. Парсить schema.table
        String schema = "dbo"; // default
        String table = tableName;
        if (tableName.contains(".")) {
            String[] parts = tableName.split("\\.", 2);
            schema = parts[0];
            table = parts[1];
        }

        // 2. Получить список колонок и определить sql_variant
        List<ColumnInfo> columns = queryInformationSchema(schema, table, dataSource);

        // 3. Сгенерировать SELECT
        StringBuilder sql = new StringBuilder("SELECT\n");

        boolean first = true;
        for (ColumnInfo col : columns) {
            if (!first) sql.append(",\n");
            first = false;

            sql.append("  ").append(col.columnName);

            if ("sql_variant".equalsIgnoreCase(col.dataType)) {
                // Добавить SQL_VARIANT_PROPERTY колонки
                sql.append(",\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName).append(", 'BaseType') as __variant_").append(col.columnName).append("_basetype,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName).append(", 'Precision') as __variant_").append(col.columnName).append("_precision,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName).append(", 'Scale') as __variant_").append(col.columnName).append("_scale,\n");
                sql.append("  SQL_VARIANT_PROPERTY(").append(col.columnName).append(", 'MaxLength') as __variant_").append(col.columnName).append("_maxlength");
            }
        }

        sql.append("\nFROM ").append(tableName);

        return sql.toString();
    }

    private List<ColumnInfo> queryInformationSchema(String schema, String table, DataSource dataSource) {
        String query =
            "SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION " +
            "FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
            "ORDER BY ORDINAL_POSITION";

        // Execute query and collect results
        // ...
    }

    private static class ColumnInfo {
        String columnName;
        String dataType;
        int ordinalPosition;
    }
}
```

### 6.2. Интегрировать в JdbcExtractor
**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcExtractor.java`

**Изменения:**
1. Проверить конфигурацию: table-based или custom query
2. Если table-based и есть sql_variant колонки → использовать SqlVariantQueryGenerator
3. Если custom query → валидировать наличие `__variant_*` колонок (см. Этап 7)

```java
public void extract(JdbcExtractorConfig config, String jobId, Consumer<EtlBatch> batchConsumer) {
    String query;

    if (config.table() != null && !config.table().isEmpty()) {
        // Table-based: автогенерация
        query = sqlVariantQueryGenerator.generateSelectQuery(config.table(), dataSource);
        log.info("Generated SQL query for table {}: {}", config.table(), query);
    } else if (config.query() != null && !config.query().isEmpty()) {
        // Custom query: использовать как есть
        query = config.query();

        // Валидация будет в Этапе 7
    } else {
        throw new ExtractionException("Either 'table' or 'query' must be specified in JDBC extractor config", jobId);
    }

    // Выполнить query с JdbcStreamingResultSetExtractor
    // ...
}
```

**Тесты:** `src/test/java/ru/pospelov/etl/engine/steps/extractor/jdbc/SqlVariantQueryGeneratorTest.java`
- [ ] Генерация для таблицы без sql_variant колонок
- [ ] Генерация для таблицы с одной sql_variant колонкой
- [ ] Генерация для таблицы с несколькими sql_variant колонками
- [ ] Парсинг schema.table
- [ ] Обработка имен с пробелами/спецсимволами (если нужно escaping)

---

## Этап 7: Валидация custom query с sql_variant (оценка: 3-4 часа)

### 7.1. Добавить валидацию в JdbcExtractor
**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcExtractor.java`

**Необходимые импорты JSqlParser:**
```java
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.JSQLParserException;
```

**Алгоритм валидации:**
1. Выполнить custom query с `WHERE 1=0` (или добавить `AND 1=0`)
2. Получить `ResultSetMetaData`
3. Проверить наличие sql_variant колонок
4. Для каждой sql_variant колонки проверить наличие `__variant_{col}_*` колонок
5. Если sql_variant колонки найдены БЕЗ `__variant_*` → выдать понятную ошибку

```java
private void validateCustomQueryForSqlVariant(String query, DataSource dataSource, String jobId) {
    // Добавить WHERE 1=0 к query
    String validationQuery = addWhereClause(query, "1=0");

    try (Connection conn = dataSource.getConnection();
         PreparedStatement ps = conn.prepareStatement(validationQuery);
         ResultSet rs = ps.executeQuery()) {

        ResultSetMetaData md = rs.getMetaData();
        List<String> sqlVariantColumns = new ArrayList<>();
        Set<String> allColumns = new HashSet<>();

        for (int i = 1; i <= md.getColumnCount(); i++) {
            String colName = md.getColumnLabel(i);
            allColumns.add(colName);

            if ("sql_variant".equals(md.getColumnTypeName(i)) && !colName.startsWith("__variant_")) {
                sqlVariantColumns.add(colName);
            }
        }

        // Проверить наличие __variant_* для каждой sql_variant колонки
        List<String> missingVariantColumns = new ArrayList<>();
        for (String variantCol : sqlVariantColumns) {
            boolean hasBasetype = allColumns.contains("__variant_" + variantCol + "_basetype");
            boolean hasPrecision = allColumns.contains("__variant_" + variantCol + "_precision");
            boolean hasScale = allColumns.contains("__variant_" + variantCol + "_scale");
            boolean hasMaxLength = allColumns.contains("__variant_" + variantCol + "_maxlength");

            if (!hasBasetype || !hasPrecision || !hasScale || !hasMaxLength) {
                missingVariantColumns.add(variantCol);
            }
        }

        if (!missingVariantColumns.isEmpty()) {
            // Построить понятное сообщение об ошибке
            StringBuilder errorMsg = new StringBuilder();
            errorMsg.append("Configuration error: sql_variant column(s) detected in query result, ");
            errorMsg.append("but required SQL_VARIANT_PROPERTY columns are missing.\n\n");

            for (String col : missingVariantColumns) {
                errorMsg.append("For column '").append(col).append("', please add:\n");
                errorMsg.append("  - SQL_VARIANT_PROPERTY(").append(col).append(", 'BaseType') as __variant_").append(col).append("_basetype\n");
                errorMsg.append("  - SQL_VARIANT_PROPERTY(").append(col).append(", 'Precision') as __variant_").append(col).append("_precision\n");
                errorMsg.append("  - SQL_VARIANT_PROPERTY(").append(col).append(", 'Scale') as __variant_").append(col).append("_scale\n");
                errorMsg.append("  - SQL_VARIANT_PROPERTY(").append(col).append(", 'MaxLength') as __variant_").append(col).append("_maxlength\n\n");
            }

            errorMsg.append("Or use 'table' configuration instead of 'query' for automatic generation.");

            throw new ValidationException(errorMsg.toString());
        }
    } catch (SQLException e) {
        throw new ExtractionException("Failed to validate custom query for sql_variant columns", jobId, null, EtlErrorSeverity.CRITICAL, e);
    }
}

private String addWhereClause(String query, String condition) {
    try {
        // Использовать JSqlParser для корректного добавления WHERE 1=0
        Statement statement = CCJSqlParserUtil.parse(query);

        if (statement instanceof Select) {
            Select select = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

            // Добавить WHERE 1=0 (или AND 1=0 если WHERE уже есть)
            Expression whereCondition = CCJSqlParserUtil.parseCondExpression(condition);

            if (plainSelect.getWhere() != null) {
                // Уже есть WHERE - добавить через AND
                AndExpression newWhere = new AndExpression(plainSelect.getWhere(), whereCondition);
                plainSelect.setWhere(newWhere);
            } else {
                // Нет WHERE - добавить новый
                plainSelect.setWhere(whereCondition);
            }

            return select.toString();
        } else {
            throw new ValidationException("Only SELECT statements are supported for custom query validation");
        }
    } catch (JSQLParserException e) {
        throw new ValidationException("Failed to parse custom SQL query: " + e.getMessage(), e);
    }
}
```

**Тесты:**
- [ ] Custom query без sql_variant → валидация проходит
- [ ] Custom query с sql_variant И с `__variant_*` → валидация проходит
- [ ] Custom query с sql_variant БЕЗ `__variant_*` → ValidationException с понятным сообщением
- [ ] Проверка сообщения об ошибке (содержит инструкции по добавлению колонок)

---

## Этап 8: Интеграционные тесты (оценка: 12-16 часов)

### 8.1. Подготовка тестовой БД
**Файл:** `src/test/resources/sql/test-schema-type-conversion.sql`

Создать тестовые таблицы с разными типами:
```sql
CREATE TABLE type_conversion_test (
    id INT PRIMARY KEY,
    date_col DATE,
    datetime2_col DATETIME2(7),
    time_col TIME(7),
    decimal_col DECIMAL(18,2),
    money_col MONEY,
    varchar_col VARCHAR(100),
    nvarchar_col NVARCHAR(100),
    varbinary_col VARBINARY(100),
    variant_col SQL_VARIANT,
    nullable_date_col DATE NULL
);

INSERT INTO type_conversion_test VALUES
(1, '2024-01-15', '2024-01-15 10:30:45.1234567', '10:30:45', 1234.56, 99.99, 'test varchar', N'test nvarchar', 0x010203, CAST(123 AS INT), NULL),
(2, '1970-01-01', '1970-01-01 00:00:00', '00:00:00', 0.01, 0, '', N'', 0x, CAST('hello' AS NVARCHAR(50)), '2024-12-31'),
(3, '2025-12-31', '2025-12-31 23:59:59.9999999', '23:59:59', 999999.99, 1000000, 'max values', N'макс значения', 0xFFFFFF, CAST(123.45 AS DECIMAL(10,2)), '2000-01-01');
```

### 8.2. Интеграционный тест: SQL -> Kafka (Avro)
**Файл:** `src/test/java/ru/pospelov/etl/engine/SqlToKafkaAvroIntegrationTest.java`

Тест-кейсы:
- [ ] DATE корректно конвертируется в Avro int (epoch days)
- [ ] DATETIME2 корректно конвертируется в Avro long (timestamp-millis)
- [ ] DECIMAL корректно конвертируется в Avro decimal (ByteBuffer)
- [ ] sql_variant с разными базовыми типами (int, decimal, nvarchar) в разных строках
- [ ] sql_variant с бинарными данными (varbinary)
- [ ] NULL значения обрабатываются корректно
- [ ] Таблица с реальными колонками `key` и `value` не конфликтует с Kafka envelope

### 8.3. Интеграционный тест: Kafka (Avro) -> SQL
**Файл:** `src/test/java/ru/pospelov/etl/engine/KafkaAvroToSqlIntegrationTest.java`

Тест-кейсы:
- [ ] Avro date (int) корректно вставляется как DATE
- [ ] Avro timestamp-millis (long) корректно вставляется как DATETIME2
- [ ] Avro decimal (ByteBuffer) корректно вставляется как DECIMAL
- [ ] sql_variant JSON-конверт корректно распаковывается и вставляется
- [ ] NULL значения в Avro корректно вставляются как NULL в SQL
- [ ] Bulk copy работает с новыми типами

### 8.4. Интеграционный тест: Round-trip (SQL -> Kafka -> SQL)
**Файл:** `src/test/java/ru/pospelov/etl/engine/RoundTripIntegrationTest.java`

Проверить что данные проходят полный цикл без потерь:
- [ ] SQL -> Kafka (Avro) -> SQL: все типы корректны
- [ ] sql_variant: базовые типы (кроме varchar/nvarchar) восстанавливаются точно
- [ ] sql_variant: varchar → nvarchar (известное ограничение) документировано в тесте

### 8.5. Тест валидации custom query
**Файл:** `src/test/java/ru/pospelov/etl/engine/CustomQueryValidationTest.java`

- [ ] Custom query с sql_variant без `__variant_*` → ValidationException
- [ ] Сообщение об ошибке содержит инструкции
- [ ] Custom query с корректными `__variant_*` → работает

### 8.6. Тест автогенерации для table-based
**Файл:** `src/test/java/ru/pospelov/etl/engine/TableBasedAutoGenerationTest.java`

- [ ] Table-based конфигурация с sql_variant → SQL автоматически генерируется
- [ ] Сгенерированный SQL содержит `SQL_VARIANT_PROPERTY` колонки
- [ ] Экстракция работает корректно

---

## Этап 9: Документация и финализация (оценка: 4-6 часов)

### 9.1. Обновить README проекта
Добавить раздел:
- Supported types и матрица конвертации (ссылка на doc/type-conversion.md)
- Работа с sql_variant
- Known Limitations (varchar → nvarchar в sql_variant)
- Примеры конфигураций (table-based и custom query с sql_variant)

### 9.2. Создать примеры конфигураций
**Файл:** `examples/sql-to-kafka-with-variant.yaml`
**Файл:** `examples/kafka-to-sql-with-variant.yaml`

### 9.3. Обновить миграционный гайд (если нужен)
Документировать breaking changes:
- `Consumer<Collection<EtlRecord>>` → `Consumer<EtlBatch>`
- `"key"/"value"` → `"__kafka_key"/"__kafka_value"`
- Как обновить существующие pipeline'ы

### 9.4. Проверить все JavaDoc
- [ ] Все новые классы имеют полную JavaDoc
- [ ] Все публичные методы документированы
- [ ] Примеры использования где уместно

### 9.5. Финальная проверка
- [ ] Запустить все тесты: `mvn clean test`
- [ ] Проверить code coverage (цель: >80% для новых классов)
- [ ] Запустить интеграционные тесты на реальной БД
- [ ] Проверить что нет TODO/FIXME в коммитах

---

## Этап 10: Code Review и Merge (оценка: 2-4 часа)

### 10.1. Подготовка к code review
- [ ] Rebase feature branch на latest main/master
- [ ] Squash commits если нужно (логические группы)
- [ ] Обновить CHANGELOG (если используется)
- [ ] Написать подробное описание PR

### 10.2. Checklist для code review
- [ ] Код соответствует требованиям из doc/type-conversion-requirements.md
- [ ] Все тесты проходят
- [ ] Нет performance regression'ов (если есть бенчмарки)
- [ ] Документация обновлена
- [ ] Breaking changes задокументированы

### 10.3. После approval
- [ ] Merge в main/master
- [ ] Создать git tag с версией (если используется семвер)
- [ ] Обновить документацию на wiki/confluence (если используется)

---

## Оценка времени и ресурсы

**Общая оценка:** 55-75 часов разработки

**Breakdown по этапам:**
- Этап 0: 1-2 часа
- Этап 1: 4-6 часов
- Этап 2: 8-12 часов (самый сложный - TypeConverter)
- Этап 3: 2-3 часа
- Этап 4: 6-8 часов (breaking changes)
- Этап 5: 8-10 часов
- Этап 6: 6-8 часов
- Этап 7: 3-4 часа
- Этап 8: 12-16 часов (интеграционные тесты)
- Этап 9: 4-6 часов
- Этап 10: 2-4 часа

**Рекомендуемый порядок выполнения:**
1. Этапы 0-3: создание базовых классов (можно параллельно)
2. Этап 2: TypeConverter (критический путь)
3. Этапы 4-5: обновление API и loader'ов (зависит от 1-2)
4. Этапы 6-7: sql_variant автогенерация и валидация
5. Этап 8: интеграционные тесты (в конце, проверяет все вместе)
6. Этапы 9-10: финализация

**Риски и митигации:**
- **Риск:** TypeConverter сложнее чем ожидается
  - **Митигация:** Начать с него, выделить больше времени
- **Риск:** Breaking changes ломают существующие pipeline'ы
  - **Митигация:** Поддержка legacy API на переходный период
- **Риск:** Интеграционные тесты выявляют проблемы поздно
  - **Митигация:** Писать unit-тесты сразу, интеграционные - инкрементально

---

## Приложение: Полезные команды

```bash
# Запустить все тесты
mvn clean test

# Запустить только unit-тесты
mvn test

# Запустить только интеграционные тесты
mvn verify -Pintegration-tests

# Проверить code coverage
mvn clean test jacoco:report
# Отчет: target/site/jacoco/index.html

# Собрать проект
mvn clean package

# Запустить один тест класс
mvn test -Dtest=TypeConverterTest

# Запустить один тест метод
mvn test -Dtest=TypeConverterTest#testLocalDateToAvroDate
```

---

## Вопросы и решения в процессе реализации

Этот раздел будет заполняться по ходу реализации для отслеживания решений.

### Q1: Нужно ли поддерживать старый API на переходный период?
**Решение:** ❌ НЕТ. Проект не в production, делаем breaking changes сразу.
- Не нужны legacy адаптеры
- Все сигнатуры меняем напрямую на `Consumer<EtlBatch>`

### Q2: Какую JSON библиотеку использовать для SqlVariantValue?
**Решение:** ✅ Jackson (уже есть в проекте)
- `com.fasterxml.jackson.databind.ObjectMapper` для сериализации/десериализации
- `jackson-datatype-jsr310` для поддержки LocalDate, Instant
- Spring Boot автоматически конфигурирует ObjectMapper bean

### Q3: Нужен ли SQL parser или достаточно простого string manipulation для WHERE 1=0?
**Решение:** ✅ Нужен SQL parser для сложных запросов
- Добавить зависимость **JSqlParser** (net.sf.jsqlparser:jsqlparser:4.9)
- Использовать для безопасного добавления `WHERE 1=0` к custom query
- Обрабатывать CTE, UNION, подзапросы корректно

---
