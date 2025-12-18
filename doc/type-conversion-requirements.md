# Требования к реализации конвертации типов SQL Server <-> Avro

Документ описывает требования к доработкам в проекте, чтобы конвертация типов работала корректно в обе стороны:
- **SQL -> Kafka (Avro)** (`sqlTableToKafka...`)
- **Kafka (Avro) -> SQL** (`kafkaToSql...`)

Это не реализация, а список требований. В требованиях учитывается текущая архитектура и конкретные классы проекта.

Связанный документ с правилами соответствия типов: `doc/type-conversion.md`.

---

## 0) Текущее состояние (для привязки требований)

Ключевые компоненты архитектуры конвертации типов:

**Extractors (источники данных)**:
- `JdbcStreamingResultSetExtractor` - читает данные через `ResultSet.getObject()`, создает `EtlBatch` с `ColumnMetadata` из `ResultSetMetaData`
- `KafkaPartitionExtractor` - читает данные из Kafka, создает `EtlBatch` с `columnMetadata = null`

**Transformers (преобразование структуры)**:
- `RecordToAvroTransformer` - подготавливает данные для Avro, сохраняет `ColumnMetadata` из источника
- `AvroToRecordTransformer` - разворачивает `GenericRecord` в плоские поля, **создает `ColumnMetadata` из Avro schema** для корректной конвертации в SQL loader'ах
- `NoopTransformer` - пробрасывает данные без изменений

**TypeConverter (конвертация типов)**:
- Централизованный класс конвертации типов: `src/main/java/ru/pospelov/etl/engine/conversion/TypeConverter.java`
- Не зависит от инфраструктуры Kafka/SQL
- Используется loader'ами для конвертации типов в момент записи
- Поддерживает канонические типы, legacy JDBC типы, строковые даты, sql_variant

**Loaders (целевые системы)**:
- `KafkaByPartitionLoader` - конвертирует Java типы в Avro физические типы через `TypeConverter.convertToAvro()`
- `JdbcLoader` - конвертирует значения в JDBC-совместимые типы через `TypeConverter.convertToJdbc()`
- `FastSqlServerLoader` - bulk copy с конвертацией типов и фильтрацией служебных полей (`__kafka_*`, `__variant_*`)

**Модель данных**:
- `EtlRecord` - содержит `Map<String, Object>` с данными
- `EtlBatch` - содержит `Collection<EtlRecord>` + `Map<String, ColumnMetadata>`
- `ColumnMetadata` - метаданные типов колонок (source: JDBC ResultSetMetaData или Avro schema)
- `SqlVariantValue` - контейнер для sql_variant значений с метаданными базового типа

---

## 1) Цели и критерии готовности

### 1.1. Цели

- Обеспечить корректную конвертацию типов на границах:
  - SQL Server -> Avro (в момент формирования Avro payload)
  - Avro -> SQL Server (в момент bind/insert)
- Перестать зависеть от "строковых дат" и "всё string".
- Поддержать `sql_variant` без вложенных Avro-схем (Avro остаётся плоским контрактом).
- Устранить конфликт имён колонок `key/value` с Kafka envelope.

### 1.2. Критерии готовности

- SQL `DATE`/`DATETIME2(p)` передаются/вставляются типобезопасно (без строковых костылей).
- `sql_variant` корректно переносится через Avro и вставляется обратно в одну SQL-колонку типа `sql_variant`.
- При несовместимости типов появляется понятная ошибка (лог содержит поле, ожидаемый тип, фактический тип/значение, контекст).
- Добавлены тесты конвертера и интеграционные тесты с corner-cases (см. раздел 6).

---

## 2) Контракт данных в pipeline

### 2.1. Поля Kafka envelope

Проблема: в текущей реализации используются служебные ключи `"key"` и `"value"` (см. `KafkaPartitionExtractor`, `KafkaByPartitionLoader`, `RecordToAvroTransformer`), что конфликтует с реальными SQL-колонками `key/value`.

Требование:
- Переименовать служебные поля Kafka envelope:
  - `__kafka_key`
  - `__kafka_value`
- Использование `key/value` как обычных колонок в SQL должно быть безопасным (не должно происходить перезаписи из-за Kafka envelope).

Затрагиваемые места (минимум):
- `src/main/java/ru/pospelov/etl/engine/steps/extractor/kafka/KafkaPartitionExtractor.java` (создание `EtlRecord` из `ConsumerRecord`)
- `src/main/java/ru/pospelov/etl/engine/steps/loader/KafkaByPartitionLoader.java` (формирование `ProducerRecord`)
- `src/main/java/ru/pospelov/etl/engine/steps/transformer/RecordToAvroTransformer.java` (перенос Kafka key и Avro value)
- Любые другие места, где читаются/пишутся `"key"`/`"value"` как служебные поля.

### 2.2. Контракт `EtlRecord`

`EtlRecord` содержит только **пользовательские данные** (колонки из SQL или поля из Kafka payload).

Структура:
```java
public class EtlRecord {
    // Системные поля (не в Map):
    private final Instant timestamp;
    private final String sourcePartition;
    private final long offset;

    // Пользовательские данные + служебные поля:
    private final Map<String, Object> fields;  // getAll() возвращает это
}
```

Соглашение о ключах в `fields`:
- `__kafka_key`, `__kafka_value` — служебные поля Kafka envelope (присутствуют только при чтении/записи Kafka)
- Остальные ключи — пользовательские колонки/поля из источника данных
- Для колонок типа `sql_variant` значение имеет тип `SqlVariantValue` (содержит все метаданные внутри объекта)

### 2.3. Контракт `EtlBatch`

Для передачи batch-level метаданных (JDBC column metadata) от extractor'ов до loader'ов вводится новый класс:

```java
public class EtlBatch {
    private final Collection<EtlRecord> records;

    // Метаданные колонок источника данных (может быть null для non-JDBC источников)
    // Ключ - имя колонки как в SQL
    private final Map<String, ColumnMetadata> columnMetadata;
}
```

Правила:
- **JDBC extractor** создаёт `EtlBatch` с заполненным `columnMetadata` (на основе `ResultSetMetaData`)
- **Kafka extractor** создаёт `EtlBatch` с `columnMetadata = null` (метаданные отсутствуют при чтении из Kafka)
- **AvroToRecordTransformer** создаёт `columnMetadata` из Avro schema при разворачивании `GenericRecord`:
  - Извлекает Avro schema из первого `GenericRecord` в batch
  - Маппирует Avro logical types на SQL type names (например, `logicalType: "date"` → `"date"`)
  - Создает `ColumnMetadata` для каждого поля с корректным `jdbcType` и `typeName`
  - Передает метаданные в `EtlBatch` для использования SQL loader'ами
- **RecordToAvroTransformer** пробрасывает `columnMetadata` без изменений (сохраняет JDBC метаданные)
- **NoopTransformer** пробрасывает `columnMetadata` без изменений
- **Loader'ы** используют `columnMetadata` для корректной конвертации типов и диагностики ошибок

### 2.4. Контракт `ColumnMetadata`

Метаданные одной колонки, извлечённые из `ResultSetMetaData`:

```java
public class ColumnMetadata {
    private final String columnName;      // имя колонки
    private final int jdbcType;           // java.sql.Types.*
    private final String typeName;        // "NVARCHAR", "DATETIME2", "sql_variant", etc.
    private final int precision;          // 0 если не применимо
    private final int scale;              // 0 если не применимо
    private final boolean nullable;
}
```

Особенности:
- Для колонок типа `sql_variant`: `typeName = "sql_variant"`, `jdbcType = Types.OTHER`
- Loader'ы определяют необходимость специальной обработки по `typeName`
- Метаданные собираются **один раз** на весь batch (не дублируются для каждой строки)

---

## 3) Конвертация дат/времени и чисел

### 3.1. SQL DATE

Требования:
- В Java канонический тип: `LocalDate`.
- В Avro: `int` + `logicalType: "date"` (epoch days).
- На стороне SQL вставка должна выполняться типобезопасно (параметр/значение должно вставляться в колонку типа `DATE` без строковых форматов).

### 3.2. SQL DATETIME2(p)

Требования:
- В Java канонический тип: `Instant`.
- В Avro: `long` + `logicalType: "timestamp-millis"`.
- Временная шкала/интерпретация: **UTC**.

Примечание (важное для реализации):
- `datetime2` не содержит timezone; при преобразовании в `Instant` должен быть единый источник "какая зона подразумевается". На первом этапе фиксируем UTC.

### 3.3. DECIMAL/NUMERIC/MONEY

Требования:
- В Java: `BigDecimal`.
- В Avro: `decimal` logicalType (на базе `bytes`), с корректными `precision/scale`.
- На SQL стороне не допускается деградация в `double` для денежных значений.

---

## 4) `sql_variant`: модель, транспорт и восстановление

### 4.1. Java-модель `SqlVariantValue`

Требования:
- Добавить Java-тип `SqlVariantValue` со следующими полями:
  - `sqlType: String` — **точно как в SQL Server** (например `int`, `decimal(18,2)`, `datetime2(7)`, `varbinary(max)`).
  - `value: String` — каноническое строковое представление значения.
  - `encoding: String` — `plain|base64|hex` (минимум `plain`; для бинарных обязателен).

**Важно:** Класс должен содержать подробную JavaDoc с объяснением:
- Почему нужен этот класс (sql_variant может содержать разные типы в разных строках)
- Как формируется `sqlType` (из SQL_VARIANT_PROPERTY)
- Как используется при сериализации в Avro (JSON-конверт)
- Как используется при десериализации и вставке в SQL (распаковка в базовый тип)
- Формат JSON-конверта и версионирование
- Правила encoding для бинарных данных

Пример:
```java
/**
 * Контейнер для значения типа SQL_VARIANT с полной информацией о базовом типе.
 *
 * SQL Server тип sql_variant может содержать значения различных базовых типов
 * (int, decimal, datetime2, varchar, varbinary и т.д.). Базовый тип может отличаться
 * в каждой строке таблицы.
 *
 * Этот класс используется как транспортный контейнер для сохранения:
 * - точного базового типа значения (с параметрами precision/scale/length)
 * - самого значения в строковом представлении
 * - encoding для бинарных данных
 *
 * Жизненный цикл:
 * 1. JDBC Extractor: читает sql_variant колонку + SQL_VARIANT_PROPERTY, создает SqlVariantValue
 * 2. Kafka Loader (Avro): сериализует в JSON-строку {"v":1,"t":"decimal(18,2)","val":"12.30","enc":"plain"}
 * 3. Kafka Extractor: десериализует JSON обратно в SqlVariantValue
 * 4. SQL Loader: распаковывает в базовый Java-тип (Integer/BigDecimal/byte[]/etc.) для JDBC вставки
 *
 * @see TypeConverter#createSqlVariant для создания из JDBC
 * @see TypeConverter#sqlVariantToJson для сериализации в Avro
 * @see TypeConverter#unpackSqlVariant для распаковки перед SQL вставкой
 */
public class SqlVariantValue {
    private final String sqlType;   // Пример: "decimal(18,2)", "datetime2(7)", "varbinary(max)"
    private final String value;     // Каноническое строковое представление
    private final String encoding;  // "plain" | "base64" | "hex"
}
```

### 4.2. Avro-транспорт (без вложенных схем)

Требования:
- В Avro схема поля `sql_variant` остаётся `string`.
- Внутри `string` хранится JSON-конверт одной строкой:
  - `{"v":1,"t":"decimal(18,2)","val":"12.30","enc":"plain"}`
  - `v` — версия формата (обязательна для будущей совместимости).

### 4.3. Получение `sqlType` из SQL Server через `SQL_VARIANT_PROPERTY`

**Проблема:** `ResultSetMetaData` сообщает только что колонка имеет тип `sql_variant`, но **не сообщает базовый тип конкретного значения** (который может отличаться в каждой строке).

**Решение:** Автогенерация SQL-запроса с вычисляемыми колонками `SQL_VARIANT_PROPERTY`.

Требования к генерации SQL:

1. **Определение sql_variant колонок:**
   - Перед формированием основного SELECT выполнить запрос к `INFORMATION_SCHEMA.COLUMNS`:
     ```sql
     SELECT COLUMN_NAME, DATA_TYPE
     FROM INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
     ```
   - Найти все колонки с `DATA_TYPE = 'sql_variant'`

2. **Генерация SELECT с SQL_VARIANT_PROPERTY:**
   - Для каждой `sql_variant` колонки добавить вычисляемые поля:
     ```sql
     SELECT
       id,
       amount,
       metadata_col,  -- sql_variant колонка
       SQL_VARIANT_PROPERTY(metadata_col, 'BaseType') as __variant_metadata_col_basetype,
       SQL_VARIANT_PROPERTY(metadata_col, 'Precision') as __variant_metadata_col_precision,
       SQL_VARIANT_PROPERTY(metadata_col, 'Scale') as __variant_metadata_col_scale,
       SQL_VARIANT_PROPERTY(metadata_col, 'MaxLength') as __variant_metadata_col_maxlength
     FROM orders
     ```
   - Префикс `__variant_{columnName}_` для вспомогательных колонок

3. **Стратегия автогенерации:**

   **Table-based конфигурация** (автогенерация):
   ```yaml
   source:
     type: jdbc
     table: orders
   ```

   Алгоритм:
   1. Выполнить запрос к `INFORMATION_SCHEMA.COLUMNS` для определения sql_variant колонок
   2. Сгенерировать полный SELECT:
      ```sql
      SELECT
        id,
        amount,
        metadata_col,
        SQL_VARIANT_PROPERTY(metadata_col, 'BaseType') as __variant_metadata_col_basetype,
        SQL_VARIANT_PROPERTY(metadata_col, 'Precision') as __variant_metadata_col_precision,
        SQL_VARIANT_PROPERTY(metadata_col, 'Scale') as __variant_metadata_col_scale,
        SQL_VARIANT_PROPERTY(metadata_col, 'MaxLength') as __variant_metadata_col_maxlength
      FROM orders
      ```

   **Custom query** (требует ручного добавления):
   ```yaml
   source:
     type: jdbc
     query: |
       WITH temp AS (
         SELECT id, metadata_col FROM orders WHERE date > '2024-01-01'
       )
       SELECT
         id,
         metadata_col,
         SQL_VARIANT_PROPERTY(metadata_col, 'BaseType') as __variant_metadata_col_basetype,
         SQL_VARIANT_PROPERTY(metadata_col, 'Precision') as __variant_metadata_col_precision,
         SQL_VARIANT_PROPERTY(metadata_col, 'Scale') as __variant_metadata_col_scale,
         SQL_VARIANT_PROPERTY(metadata_col, 'MaxLength') as __variant_metadata_col_maxlength
       FROM temp
       WHERE amount > 100
       GROUP BY category
       HAVING COUNT(*) > 5
       ORDER BY total DESC
   ```

   **Валидация custom query:**
   - Выполнить query с `WHERE 1=0` для получения `ResultSetMetaData`
   - Проверить наличие sql_variant колонок в результате
   - Если найдены sql_variant колонки БЕЗ соответствующих `__variant_{col}_*` колонок:
     - **Выдать ошибку** с понятным сообщением:
       ```
       Configuration error: sql_variant column 'metadata_col' detected in query result,
       but required SQL_VARIANT_PROPERTY columns are missing.

       Please add the following columns to your SELECT:
       - SQL_VARIANT_PROPERTY(metadata_col, 'BaseType') as __variant_metadata_col_basetype
       - SQL_VARIANT_PROPERTY(metadata_col, 'Precision') as __variant_metadata_col_precision
       - SQL_VARIANT_PROPERTY(metadata_col, 'Scale') as __variant_metadata_col_scale
       - SQL_VARIANT_PROPERTY(metadata_col, 'MaxLength') as __variant_metadata_col_maxlength

       Or use 'table' configuration instead of 'query' for automatic generation.
       ```

   **Обоснование:**
   - Custom query может содержать сложные конструкции (CTE, UNION, GROUP BY, HAVING, ORDER BY, подзапросы)
   - Автоматическая модификация таких запросов рискованна и может привести к ошибкам
   - Явное указание колонок дает пользователю полный контроль
   - Для простых случаев доступна table-based конфигурация с автогенерацией

### 4.4. Обработка `sql_variant` в JDBC Extractor

Логика работы `JdbcStreamingResultSetExtractor`:

1. **Собрать ColumnMetadata для пользовательских колонок** (исключая служебные `__variant_*`)
   - Служебные колонки `__variant_*` остаются в физическом ResultSet для чтения метаданных
   - Но не включаются в `ColumnMetadata`, так как не представляют пользовательские данные
2. **Определить sql_variant колонки** по наличию соответствующих `__variant_*` полей в `ResultSetMetaData`
3. **При чтении каждой строки:**
   ```java
   for (int i = 1; i <= columnCount; i++) {
       String colName = metadata.getColumnName(i);

       // Пропустить вспомогательные колонки - они не добавляются в EtlRecord
       if (colName.startsWith("__variant_")) {
           continue;
       }

       ColumnMetadata colMeta = columnMetadata.get(colName);

       // Обычная колонка
       Object value = rs.getObject(colName);

       // Если это sql_variant колонка, читаем метаданные из текущей строки
       if (isSqlVariantColumn(colName)) {
           // Читаем properties из вспомогательных колонок текущей строки
           String baseType = rs.getString("__variant_" + colName + "_basetype");
           Integer precision = getIntOrNull(rs, "__variant_" + colName + "_precision");
           Integer scale = getIntOrNull(rs, "__variant_" + colName + "_scale");
           Integer maxLength = getIntOrNull(rs, "__variant_" + colName + "_maxlength");

           // Создаем SqlVariantValue через TypeConverter
           SqlVariantValue variant = typeConverter.createSqlVariant(
               value, baseType, precision, scale, maxLength
           );

           record.put(colName, variant);
       } else {
           record.put(colName, value);
       }
   }
   ```

4. **Вспомогательные колонки `__variant_*`:**
   - Присутствуют в физическом ResultSet
   - Используются для чтения метаданных каждой строки
   - НЕ добавляются в `EtlRecord.fields`
   - НЕ включаются в `ColumnMetadata` batch'а

### 4.5. Восстановление и вставка обратно в SQL

Требования:
- **Kafka -> SQL (через Avro):**
  - Avro Extractor читает JSON-строку, десериализует в `SqlVariantValue` через `TypeConverter.sqlVariantFromJson()`
  - SQL Loader определяет тип значения по `instanceof SqlVariantValue`
  - Вызывает `TypeConverter.unpackSqlVariant()` для распаковки в базовый Java-тип
  - Вставляет базовый тип через JDBC (SQL Server автоматически упакует в sql_variant с правильным базовым типом)

- **varchar vs nvarchar (принятое ограничение):**
  - `SqlVariantValue.sqlType` содержит точный тип (`varchar(50)` vs `nvarchar(50)`) для информации
  - При распаковке `TypeConverter.unpackSqlVariant()` возвращает базовый Java-тип (`String` для строк, `Integer` для чисел, etc.)
  - **Bulk copy ограничение:** При вставке `String` в sql_variant через bulk copy, SQL Server сам выбирает `nvarchar`
  - **Принятое решение:** `varchar` → `nvarchar`, `char` → `nchar` (функционально эквивалентно, потеря только в памяти)
  - Ограничение документируется в JavaDoc `SqlVariantValue` и в документации проекта

---

## 5) Где выполняется конвертация (с учётом текущей реализации)

Требование: конвертация должна выполняться там, где известен целевой контракт и можно дать понятную ошибку.

**Архитектурное решение:**
- Конвертация выполняется в **loader'ах** (там, где известен целевой контракт)
- Transformer'ы используются только для бизнес-преобразований (агрегация, фильтрация, обогащение данных)
- Вся логика приведения типов вынесена в отдельный класс `TypeConverter` (без зависимостей от Kafka/SQL/Avro инфраструктуры)

**Критически важно:** Для корректной конвертации Avro физических типов (например, `int` для date) в SQL-совместимые Java типы, необходимы метаданные типов:
- **JDBC → Kafka:** метаданные берутся из `ResultSetMetaData` JDBC источника
- **Kafka → SQL:** метаданные создаются из Avro schema в `AvroToRecordTransformer`
- Без метаданных TypeConverter не может отличить "просто Integer" от "Avro date (int epoch days)" и конвертация в LocalDate не произойдет

### 5.1. Контракт `TypeConverter`

Отдельный класс для конвертации типов между Java, Avro и JDBC представлениями.

```java
/**
 * Конвертер типов для ETL pipeline.
 * Выполняет приведение типов между Java, Avro и JDBC представлениями.
 * Не зависит от инфраструктуры Kafka/SQL - только чистая логика конвертации.
 */
public class TypeConverter {

    // ============ SQL -> Avro (для Kafka Loader с format=AVRO) ============

    /**
     * Конвертирует Java-значение в Avro-совместимый тип согласно Avro schema.
     *
     * @param javaValue значение из EtlRecord (может быть LocalDate, Instant, BigDecimal, SqlVariantValue, etc.)
     * @param avroField поле Avro схемы (содержит physical type + logicalType)
     * @return значение, совместимое с физическим типом Avro schema
     * @throws TypeConversionException если конвертация невозможна
     */
    Object convertToAvro(Object javaValue, Schema.Field avroField);

    // ============ Avro/Any -> JDBC (для SQL Loader) ============

    /**
     * Конвертирует значение в JDBC-совместимый тип для PreparedStatement/BulkCopy.
     *
     * @param value значение из EtlRecord (может быть результатом convertToAvro или нативный Java-тип)
     * @param targetMetadata метаданные целевой SQL колонки (может быть null если недоступно)
     * @return значение, готовое к вставке через JDBC
     * @throws TypeConversionException если конвертация невозможна
     */
    Object convertToJdbc(Object value, ColumnMetadata targetMetadata);

    // ============ sql_variant специфичные методы ============

    /**
     * Создает SqlVariantValue из JDBC значения и SQL_VARIANT_PROPERTY метаданных.
     *
     * @param jdbcValue значение из ResultSet.getObject() для sql_variant колонки
     * @param baseType значение SQL_VARIANT_PROPERTY(col, 'BaseType')
     * @param precision значение SQL_VARIANT_PROPERTY(col, 'Precision') или null
     * @param scale значение SQL_VARIANT_PROPERTY(col, 'Scale') или null
     * @param maxLength значение SQL_VARIANT_PROPERTY(col, 'MaxLength') или null
     * @return SqlVariantValue с заполненным sqlType в формате SQL Server
     */
    SqlVariantValue createSqlVariant(
        Object jdbcValue,
        String baseType,
        Integer precision,
        Integer scale,
        Integer maxLength
    );

    /**
     * Распаковывает SqlVariantValue в базовый JDBC-совместимый Java-тип.
     *
     * @param variant контейнер sql_variant
     * @return базовый Java-тип (Integer, BigDecimal, String, byte[], java.sql.Timestamp, etc.)
     * @throws TypeConversionException если базовый тип не поддерживается или значение невалидно
     */
    Object unpackSqlVariant(SqlVariantValue variant);

    /**
     * Сериализует SqlVariantValue в JSON-строку для хранения в Avro.
     *
     * @param variant контейнер sql_variant
     * @return JSON-строка вида {"v":1,"t":"decimal(18,2)","val":"12.30","enc":"plain"}
     */
    String sqlVariantToJson(SqlVariantValue variant);

    /**
     * Десериализует JSON-строку обратно в SqlVariantValue.
     *
     * @param json JSON-строка из Avro
     * @return SqlVariantValue
     * @throws TypeConversionException если JSON невалиден или версия не поддерживается
     */
    SqlVariantValue sqlVariantFromJson(String json);
}
```

**Правила конвертации в `convertToAvro`:**
- **Канонические Java time типы:**
  - `LocalDate` → `Integer` (epoch days) для `logicalType: "date"`
  - `Instant` → `Long` (epoch millis) для `logicalType: "timestamp-millis"`
  - `LocalTime` → `Long` (nanoseconds) для `logicalType: "time-micros"`
- **JDBC legacy типы** (для совместимости с JDBC драйверами):
  - `java.sql.Date` → `Integer` (epoch days) для `logicalType: "date"`
  - `java.sql.Timestamp` → `Long` (epoch millis) для `logicalType: "timestamp-millis"`
  - `java.sql.Time` → `Long` (nanoseconds) для `logicalType: "time-micros"`
- **Строковые даты** (если драйвер возвращает строки):
  - `String` → парсится в `LocalDate` → `Integer` для `logicalType: "date"`
  - `String` → парсится в `Instant` → `Long` для `logicalType: "timestamp-millis"`
- **Прочие типы:**
  - `BigDecimal` → `ByteBuffer` для `logicalType: "decimal"`
  - `SqlVariantValue` → `String` (через `sqlVariantToJson()`)
  - `byte[]` → `ByteBuffer` для Avro `bytes`
  - Остальные типы → проверка совместимости с физическим типом Avro schema

**Правила конвертации в `convertToJdbc`:**
- **Avro logical types** (требуют `ColumnMetadata` для определения типа):
  - `Integer` (Avro date) → `LocalDate` (если `metadata.typeName == "date"`)
  - `Long` (Avro timestamp-millis) → `Instant` (если `metadata.typeName == "datetime2"`)
  - `ByteBuffer` (Avro decimal) → `BigDecimal`
- **Канонические типы для JDBC/BulkCopy:**
  - `LocalDate` → `java.sql.Date`
  - `Instant` → `java.sql.Timestamp`
  - `LocalTime` → `java.sql.Time`
- **sql_variant:**
  - `String` (если распознан JSON sql_variant) → распаковка через `sqlVariantFromJson()` + `unpackSqlVariant()`
  - `SqlVariantValue` → базовый тип через `unpackSqlVariant()`
- **Прочие:**
  - `ByteBuffer` → `byte[]`
  - `GenericRecord` → ошибка (должен быть развернут в transformer'е `AvroToRecordTransformer`)

### 5.2. SQL -> Kafka (Avro) - конвертация в Kafka Loader

Требования:
- **Kafka Loader с format=AVRO** выполняет конвертацию в момент сборки `GenericRecord`
- Алгоритм:
  ```java
  Schema schema = schemaRegistryService.getLatestSchema(config.avroSchemaSubject());
  GenericRecord gr = new GenericData.Record(schema);

  for (Schema.Field field : schema.getFields()) {
      Object rawValue = record.get(field.name());
      if (rawValue == null) {
          gr.put(field.name(), null);
          continue;
      }

      // Конвертация через TypeConverter
      Object avroValue = typeConverter.convertToAvro(rawValue, field);
      gr.put(field.name(), avroValue);
  }

  // Публикация в Kafka
  String key = record.get("__kafka_key");
  producer.send(new ProducerRecord<>(topic, key, gr));
  ```

- Transformer `RecordToAvroTransformer` становится deprecated/опциональным (на переходный период можно оставить для совместимости)

### 5.3. Kafka (Avro) -> SQL - конвертация в SQL Loader

Требования:
- **SQL Loader (JDBC и BulkCopy)** выполняет конвертацию перед вставкой
- Алгоритм для PreparedStatement:
  ```java
  for (String columnName : record.getAll().keySet()) {
      if (columnName.startsWith("__kafka_")) continue; // пропустить служебные поля

      Object rawValue = record.get(columnName);
      ColumnMetadata meta = batch.getColumnMetadata().get(columnName);

      Object jdbcValue = typeConverter.convertToJdbc(rawValue, meta);
      ps.setObject(columnIndex++, jdbcValue);
  }
  ```

- Алгоритм для BulkCopy аналогичен, но также требуется обновление `EtlBulkRecord.mapJavaToSqlType`

### 5.4. Обновление `EtlBulkRecord.mapJavaToSqlType`

Требования:
- Добавить поддержку Java time-типов:
  ```java
  if (value instanceof LocalDate) return Types.DATE;
  if (value instanceof Instant) return Types.TIMESTAMP;
  if (value instanceof LocalTime) return Types.TIME;
  if (value instanceof OffsetDateTime) return Types.TIMESTAMP_WITH_TIMEZONE;
  ```

- Добавить поддержку sql_variant (после распаковки):
  ```java
  // sql_variant уже распакован в базовый тип через TypeConverter.unpackSqlVariant
  // Определяем тип по базовому значению
  if (value instanceof Integer) return Types.INTEGER;
  if (value instanceof BigDecimal) return Types.DECIMAL;
  // ... и т.д.
  ```

### 5.5. Bulk copy: фильтрация служебных полей

Требования:
- SQL bulk copy должен маппить **только пользовательские колонки**, исключая:
  - `__kafka_key`, `__kafka_value` (служебные поля Kafka)
  - Колонки с префиксом `__variant_*` (не должны попасть в EtlRecord, но на всякий случай фильтруем)

Алгоритм в `FastSqlServerLoader`:
```java
Set<String> userColumns = batch.getRecords().get(0).getAll().keySet().stream()
    .filter(k -> !k.startsWith("__kafka_"))
    .filter(k -> !k.startsWith("__variant_"))
    .collect(Collectors.toSet());

for (String columnName : userColumns) {
    sqlBulkCopy.addColumnMapping(columnName, columnName);
}

// При добавлении строк также фильтровать
for (EtlRecord record : batch.getRecords()) {
    Object[] rowValues = userColumns.stream()
        .map(col -> {
            Object rawValue = record.get(col);
            ColumnMetadata meta = batch.getColumnMetadata().get(col);
            return typeConverter.convertToJdbc(rawValue, meta);
        })
        .toArray();

    sqlBulkCopy.writeToServer(rowValues);
}
```

---

## 6) Логи/диагностика

Требования:
- При ошибках конвертации логировать:
  - имя поля,
  - ожидаемый тип/контракт (Avro schema или ожидаемый SQL тип),
  - фактический Java-тип и превью значения,
  - контекст (`jobId`, `partition`, `offset` и т.п., если доступно).

Формат (1 строка):
- `Type mismatch at <stage>: field=... expected=... actual=... value=... job=... partition=... offset=...`

---

## 7) Тесты (обязательно)

### 7.1. Unit-тесты конвертера

Требования:
- Добавить unit-тесты для конвертера (логики приведения типов) с фокусом на corner-cases, а не на объём данных.

Обязательные кейсы:
- `null` значения (включая nullable unions в Avro).
- `DATE` <-> Avro `date` (epoch days), включая границы (например 1970-01-01).
- `DATETIME2` <-> Avro `timestamp-millis` с UTC.
- `BigDecimal` <-> Avro `decimal` (scale/precision).
- `sql_variant`:
  - разные базовые типы в разных строках (например `int` и `nvarchar`),
  - бинарный вариант (base64/hex),
  - невалидный JSON/невалидное значение для указанного `sqlType` -> ожидаемая ошибка.

### 7.2. Интеграционные тесты pipeline

Требования:
- Добавить интеграционные тесты для обоих направлений:
  - **SQL -> Kafka (Avro)** с полями `DATE`, `DATETIME2`, `DECIMAL`, `sql_variant`, `null`.
  - **Kafka (Avro) -> SQL** с теми же кейсами.
- Объём данных не важен; важны исключительные случаи и корректные ошибки.

Дополнительно:
- Отдельный тест на отсутствие конфликта имён: таблица с реальными колонками `key` и `value` должна корректно переноситься через pipeline (Kafka envelope не должен их затирать после переименования в `__kafka_*`).

---

## 8) Изменения API и breaking changes

### 8.1. Изменение сигнатур extractor/transformer/loader

**Breaking change:** Переход с `Consumer<Collection<EtlRecord>>` на `Consumer<EtlBatch>`.

Затрагиваемые интерфейсы и классы:
- `JdbcStreamingResultSetExtractor.extract(..., Consumer<EtlBatch> batchConsumer)`
- `KafkaPartitionExtractor.extract(..., Consumer<EtlBatch> batchConsumer)`
- Все transformer'ы: `transform(EtlBatch batch)` → возвращает `EtlBatch`
- Все loader'ы: `load(Config config, String jobId, EtlBatch batch)`

**Статус:** Приемлемо, так как проект еще не в production.

**Миграция:**
- На переходный период можно поддержать оба варианта через метод-адаптер:
  ```java
  default Consumer<Collection<EtlRecord>> asLegacyConsumer(Consumer<EtlBatch> batchConsumer) {
      return records -> batchConsumer.accept(new EtlBatch(records, null));
  }
  ```
- После завершения миграции убрать legacy методы.

### 8.2. Решенные вопросы из предыдущей версии документа

**1) `sql_variant` varchar vs nvarchar:**
- **Решение:** При вставке через bulk copy SQL Server сам выбирает тип для строк (`nvarchar`).
- **Принятое ограничение:** `varchar` → `nvarchar`, `char` → `nchar` (функционально эквивалентно).
- Ограничение документируется в JavaDoc и документации проекта.

**2) Стратегия `SQL_VARIANT_PROPERTY`:**
- **Решение:** Автогенерация SQL с `SQL_VARIANT_PROPERTY` только для **table-based** конфигурации.
- Для **custom query**: пользователь вручную добавляет `SQL_VARIANT_PROPERTY` колонки если есть sql_variant.
- Валидация на старте: если в custom query найдены sql_variant колонки БЕЗ `__variant_*` колонок → ошибка с подробной инструкцией.

**3) Tombstones в Kafka:**
- **Текущее поведение:** `KafkaPartitionExtractor` пропускает записи с `record.value() == null` (строка 140-143 в `KafkaPartitionExtractor.java`).
- **Решение:** Оставить без изменений. Tombstones не являются обычными данными и должны обрабатываться специально (если потребуется - отдельная задача).

### 8.3. Новые ограничения и требования

**1) Служебные префиксы:**
- `__kafka_*` — зарезервировано для Kafka envelope
- `__variant_*` — зарезервировано для sql_variant метаданных
- Пользовательские колонки с такими префиксами не поддерживаются (документировать как ограничение).

**2) Custom query с sql_variant:**
- Автогенерация `SQL_VARIANT_PROPERTY` работает **только для table-based** конфигурации
- Для **custom query** пользователь обязан вручную добавить `SQL_VARIANT_PROPERTY` колонки
- Валидация при старте: если обнаружен sql_variant без `__variant_*` колонок → ошибка с инструкцией
- Обоснование: custom query может содержать CTE, UNION, GROUP BY, подзапросы - автоматическая модификация рискованна

**3) Ограничение varchar → nvarchar для sql_variant:**
- При вставке через bulk copy: `varchar` → `nvarchar`, `char` → `nchar`
- Функционально эквивалентно, потеря только в памяти (2 байта вместо 1 на символ)
- Документируется в JavaDoc и Known Limitations проекта
