# План рефакторинга конвертации типов

Дата создания: 2025-12-17

## Цель

Устранить технические недоработки в реализации конвертации типов, выявленные в `type-conversion-audit-report.md`, для обеспечения полного соответствия требованиям из `type-conversion.md` и `type-conversion-requirements.md`.

## Приоритет задач

**P0 (критично)** - функционал не работает end-to-end
**P1 (высокий)** - работает, но с ошибками/неполнотой
**P2 (средний)** - технический долг, архитектурные улучшения

---

## Stage 1: Очистка ColumnMetadata от служебных полей

**Приоритет:** P2 (средний)
**Статус:** Готово к реализации

### Проблема

`ColumnMetadata` включает служебные колонки `__variant_*`, которые:
- Засоряют метаданные batch'а
- Не используются после первоначального обнаружения sql_variant
- Противоречие в требованиях (раздел 4.4 в `type-conversion-requirements.md`)

### Решение

Исключить `__variant_*` из `ColumnMetadata` при сборе метаданных.

### Изменения

**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcStreamingResultSetExtractor.java`

**Метод:** `buildColumnMetadata(ResultSetMetaData md)`

```java
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
```

### Обоснование

- `__variant_*` колонки остаются в физическом ResultSet и используются через `ResultSetMetaData`
- Метод `detectSqlVariantColumns()` строит `allColumns` из `ResultSetMetaData`, а не из `columnMetadata`
- `ColumnMetadata` будет содержать только метаданные реальных колонок таблицы

### Тестирование

- Проверить, что `detectSqlVariantColumns()` продолжает работать корректно
- Проверить, что `createSqlVariantValue()` читает метаданные из текущей строки ResultSet
- Интеграционный тест: SQL с sql_variant → Kafka → SQL (round-trip)

---

## Stage 2: Канонизация типов в JDBC Extractor

**Приоритет:** P0 (критично)
**Статус:** Требует реализации

### Проблема

JDBC extractor использует `ResultSet.getObject()` без нормализации типов:
- `DATE` возвращает `java.sql.Date`, а требуется `LocalDate`
- `DATETIME2` возвращает `java.sql.Timestamp`, а требуется `Instant`
- `DECIMAL` может вернуть driver-specific тип, а требуется `BigDecimal`

В итоге:
- Kafka loader (Avro) получает неканонические типы и не может конвертировать
- SQL loader не может полагаться на типы значений

### Решение

Добавить нормализацию типов в `JdbcStreamingResultSetExtractor` сразу после `rs.getObject()`.

### Изменения

**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcStreamingResultSetExtractor.java`

**Новый метод:** `normalizeJdbcValue(Object value, ColumnMetadata meta)`

```java
/**
 * Normalizes JDBC value to canonical Java type according to SQL type.
 *
 * <p>Conversions:
 * <ul>
 * <li>java.sql.Date → LocalDate</li>
 * <li>java.sql.Timestamp → Instant (UTC)</li>
 * <li>java.sql.Time → LocalTime</li>
 * <li>Driver-specific numeric → BigDecimal (for DECIMAL/NUMERIC/MONEY)</li>
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
            && !(value instanceof BigDecimal)) {
        // Handle driver-specific types
        if (value instanceof Number) {
            return new BigDecimal(value.toString());
        }
    }

    return value;
}
```

**Обновить основной цикл чтения:**

```java
for (int i = 1; i <= md.getColumnCount(); i++) {
    String col = md.getColumnLabel(i);

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
```

### Тестирование

- Unit-тест: проверка нормализации для каждого типа
- Интеграционный тест: DATE/DATETIME2/DECIMAL из SQL → проверить типы в EtlRecord

---

## Stage 3: Обратная конвертация Avro logical types в TypeConverter

**Приоритет:** P0 (критично)
**Статус:** Требует реализации

### Проблема

`TypeConverter.convertToJdbc()` не обрабатывает обратную конвертацию Avro logical types:
- Avro `int` (date) → `java.sql.Date` / `LocalDate`
- Avro `long` (timestamp-millis) → `java.sql.Timestamp` / `Instant`
- Avro `ByteBuffer` (decimal) → `BigDecimal`

Направление Kafka(Avro) → SQL не работает для logical types.

### Решение

Доработать `TypeConverter.convertToJdbc()` для распознавания и конвертации Avro значений.

### Изменения

**Файл:** `src/main/java/ru/pospelov/etl/engine/conversion/TypeConverter.java`

**Метод:** `convertToJdbc(Object value, ColumnMetadata targetMetadata, String jobId)`

**Добавить обработку до существующих проверок:**

```java
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

    // ========== НОВОЕ: Обработка Avro logical types ==========

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
        ByteBuffer buffer = (ByteBuffer) value;

        // Avro decimal encoding: unscaled value in big-endian byte order
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        BigInteger unscaledValue = new BigInteger(bytes);
        int scale = targetMetadata.getScale();

        return new BigDecimal(unscaledValue, scale);
    }

    // ========== Существующая логика ==========

    // Обработка LocalDate → java.sql.Date
    if (value instanceof LocalDate) {
        return Date.valueOf((LocalDate) value);
    }

    // ... остальной код без изменений
}
```

### Обоснование

- `targetMetadata` позволяет определить целевой SQL тип и корректно распаковать Avro значение
- Для decimal нужны `precision/scale` из metadata для правильной распаковки
- Порядок проверок важен: сначала Avro types, потом canonical Java types

### Тестирование

- Unit-тест: Avro int (epoch days) → LocalDate
- Unit-тест: Avro long (epoch millis) → Instant
- Unit-тест: Avro ByteBuffer (decimal) → BigDecimal с правильным scale
- Интеграционный тест: Kafka(Avro) → SQL → проверка корректности вставки

---

## Stage 4: Исправление тестов

**Приоритет:** P1 (высокий)
**Статус:** Требует реализации

### Проблема 1: Некорректный тест в TypeConverterTest

**Файл:** `src/test/java/ru/pospelov/etl/engine/conversion/TypeConverterTest.java`

**Тест:** `testFullPipeline_SqlToKafkaToSql_Date`

**Проблема:** Тест проверяет конвертацию `originalDate` вместо результата Avro (Integer):

```java
// SQL Loader: Convert back to SQL type
Object sqlValue = converter.convertToJdbc(originalDate, null, "test-job"); // ← НЕПРАВИЛЬНО
```

**Исправление:**

```java
@Test
void testFullPipeline_SqlToKafkaToSql_Date() throws Exception {
    TypeConverter converter = new TypeConverter();
    LocalDate originalDate = LocalDate.of(2024, 3, 15);

    // Step 1: JDBC Extractor (канонический тип уже LocalDate)
    assertEquals(originalDate, originalDate);

    // Step 2: Kafka Loader - Convert to Avro
    Schema.Field dateField = createAvroField("order_date",
        LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));

    Object avroValue = converter.convertToAvro(originalDate, dateField, "test-job");
    assertEquals(Integer.class, avroValue.getClass());
    assertEquals((int) originalDate.toEpochDay(), avroValue);

    // Step 3: Kafka Extractor reads int from Avro
    Integer avroDate = (Integer) avroValue;

    // Step 4: SQL Loader - Convert back to JDBC type
    ColumnMetadata dateMetadata = new ColumnMetadata("order_date", Types.DATE, "DATE", 0, 0, true);
    Object sqlValue = converter.convertToJdbc(avroDate, dateMetadata, "test-job");

    // Should be LocalDate (canonical type for PreparedStatement/BulkCopy)
    assertEquals(LocalDate.class, sqlValue.getClass());
    assertEquals(originalDate, sqlValue);
}
```

### Проблема 2: Отсутствующие тесты

Добавить unit-тесты:

**Файл:** `src/test/java/ru/pospelov/etl/engine/conversion/TypeConverterTest.java`

```java
@Test
void testAvroDateToLocalDate() {
    TypeConverter converter = new TypeConverter();
    Integer avroDate = (int) LocalDate.of(2024, 3, 15).toEpochDay();
    ColumnMetadata meta = new ColumnMetadata("order_date", Types.DATE, "DATE", 0, 0, true);

    Object result = converter.convertToJdbc(avroDate, meta, "test-job");

    assertEquals(LocalDate.class, result.getClass());
    assertEquals(LocalDate.of(2024, 3, 15), result);
}

@Test
void testAvroTimestampToInstant() {
    TypeConverter converter = new TypeConverter();
    Instant expected = Instant.parse("2024-03-15T10:30:45.123Z");
    Long avroTimestamp = expected.toEpochMilli();
    ColumnMetadata meta = new ColumnMetadata("created_at", Types.TIMESTAMP, "DATETIME2", 7, 0, true);

    Object result = converter.convertToJdbc(avroTimestamp, meta, "test-job");

    assertEquals(Instant.class, result.getClass());
    assertEquals(expected, result);
}

@Test
void testAvroDecimalToBigDecimal() {
    TypeConverter converter = new TypeConverter();
    BigDecimal expected = new BigDecimal("12345.67");

    // Encode as Avro decimal
    BigInteger unscaled = expected.unscaledValue();
    ByteBuffer avroDecimal = ByteBuffer.wrap(unscaled.toByteArray());

    ColumnMetadata meta = new ColumnMetadata("amount", Types.DECIMAL, "DECIMAL", 18, 2, true);

    Object result = converter.convertToJdbc(avroDecimal, meta, "test-job");

    assertEquals(BigDecimal.class, result.getClass());
    assertEquals(expected, result);
}

@Test
void testJdbcDateNormalization() {
    // Проверка, что java.sql.Date конвертируется в LocalDate
    // (будет использоваться в Stage 2)
}

@Test
void testJdbcTimestampNormalization() {
    // Проверка, что java.sql.Timestamp конвертируется в Instant
}
```

### Проблема 3: Тест на отсутствие конфликта имен

**Файл:** `src/test/java/ru/pospelov/etl/engine/KeyValueColumnConflictTest.java` (новый)

```java
@SpringBootTest
class KeyValueColumnConflictTest {

    @Test
    void testTableWithKeyValueColumns() {
        // Создать таблицу с колонками key и value
        // SQL -> Kafka -> SQL
        // Проверить, что колонки не затираются Kafka envelope (__kafka_key, __kafka_value)
    }
}
```

---

## Stage 5: Перенос JdbcExtractorValidationTest в интеграционные

**Приоритет:** P1 (высокий)
**Статус:** Требует реализации

### Проблема

`JdbcExtractorValidationTest` помечен `@SpringBootTest` и использует SQL Server, но имя не содержит `*IntegrationTest`, поэтому:
- Не исключается Maven Surefire
- Падает при `mvn test` без локального SQL Server

### Решение

**Вариант A:** Переименовать в `JdbcExtractorValidationIntegrationTest`

```bash
git mv src/test/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcExtractorValidationTest.java \
         src/test/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcExtractorValidationIntegrationTest.java
```

**Вариант B:** Добавить в `pom.xml`:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <configuration>
        <excludes>
            <exclude>**/*IntegrationTest.java</exclude>
            <exclude>**/JdbcExtractorValidationTest.java</exclude>
        </excludes>
    </configuration>
</plugin>
```

**Рекомендация:** Вариант A (переименование) - более явный и стандартный подход.

---

## Stage 6: Обновление документации

**Приоритет:** P2 (средний)
**Статус:** Требует обновления после реализации

### Изменения в `doc/type-conversion-requirements.md`

**Раздел 2.4 (строка 321):** Убрать противоречие

**Было:**
```
1. **Собрать ColumnMetadata для всех колонок** (включая основные и вспомогательные `__variant_*`)
...
4. **Вспомогательные колонки `__variant_*` НЕ добавляются** в `EtlRecord.fields` и НЕ включаются в `ColumnMetadata` batch'а
```

**Стало:**
```
1. **Собрать ColumnMetadata для пользовательских колонок** (исключая служебные `__variant_*`)
2. **Определить sql_variant колонки** по наличию `__variant_*` полей в `ResultSetMetaData`
3. **При чтении каждой строки:**
   - Пропустить вспомогательные колонки `__variant_*` (они не добавляются в `EtlRecord`)
   - Для каждой sql_variant колонки прочитать метаданные из текущей строки ResultSet
   - Создать `SqlVariantValue` через `TypeConverter.createSqlVariant()`
```

### Изменения в `doc/type-conversion.md`

**Раздел 2.1 (строка 143):** Уточнить про ColumnMetadata

**Было:**
```
- JDBC-метаданные колонок (typeName/precision/scale/...) собираются один раз на ResultSet/партицию
  и хранятся на уровне batch/партиции (один объект на batch), а не в каждой записи.
```

**Стало:**
```
- JDBC-метаданные колонок (typeName/precision/scale/...) собираются один раз на ResultSet
  и хранятся в `EtlBatch.columnMetadata` (один объект на batch).
- Служебные колонки `__variant_*` исключаются из `ColumnMetadata`, так как они являются
  внутренней деталью реализации sql_variant и не представляют пользовательские данные.
```

**Раздел 2.1 (строка 25):** Добавить про канонизацию

**Добавить после строки 28:**
```
- **Канонизация JDBC типов** выполняется в JDBC extractor сразу после `ResultSet.getObject()`:
  - `java.sql.Date` → `LocalDate`
  - `java.sql.Timestamp` → `Instant` (UTC)
  - `java.sql.Time` → `LocalTime`
  - Driver-specific numeric → `BigDecimal` (для DECIMAL/NUMERIC/MONEY)
- Это гарантирует, что дальше по pipeline типы стабильны и предсказуемы.
```

---

## Порядок реализации

1. **Stage 1** - Исключить `__variant_*` из ColumnMetadata (низкий риск, быстро)
2. **Stage 5** - Переименовать JdbcExtractorValidationTest (низкий риск, быстро)
3. **Stage 4** - Исправить существующие тесты (средний риск)
4. **Stage 2** - Канонизация типов в JDBC Extractor (высокий риск, критично)
5. **Stage 3** - Обратная конвертация Avro logical types (высокий риск, критично)
6. **Stage 4 (продолжение)** - Добавить новые тесты
7. **Stage 6** - Обновить документацию

## Критерии готовности

- ✅ Все unit-тесты проходят
- ✅ Интеграционные тесты проходят (SQL → Kafka → SQL round-trip)
- ✅ `mvn test` выполняется успешно без внешних зависимостей
- ✅ `mvn verify -Pintegration-tests` выполняется успешно с Docker/TestContainers
- ✅ Документация обновлена и не содержит противоречий

## Риски

| Риск | Вероятность | Влияние | Митигация |
|------|-------------|---------|-----------|
| Сломать существующие pipeline | Средняя | Высокое | Полное покрытие тестами перед изменениями |
| Несовместимость с MS SQL Server драйвером | Низкая | Среднее | Тестирование с разными версиями драйвера |
| Потеря precision при Avro decimal | Низкая | Высокое | Unit-тесты с граничными значениями |

---

**Итого задач:** 6 stages
**Оценка времени:** 3-5 дней (при последовательной реализации)
**Ответственный:** [Указать имя разработчика]
