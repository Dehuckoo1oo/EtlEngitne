# План: Поддержка работы с таблицами из других БД + Исправление TypeConversionIntegrationTest

## Оглавление
- [Краткое резюме](#краткое-резюме)
- [Диагноз проблемы](#диагноз-проблемы)
- [Решение](#решение)
- [Критические файлы для изменения](#критические-файлы-для-изменения)
- [План выполнения](#план-выполнения)
- [Ожидаемый результат](#ожидаемый-результат)
- [Риски и совместимость](#риски-и-совместимость)

---

## Краткое резюме

### Цель
Добавить полноценную поддержку работы с таблицами из других баз данных на том же SQL Server и исправить падающие тесты TypeConversionIntegrationTest.

### Найдено критических ошибок

**P0-1: SqlVariantQueryGenerator - неправильный парсинг 3-частных имен таблиц**
- Тесты используют `SUPPORT.dbo.type_conversion_src` (database.schema.table)
- Код парсит это как schema="SUPPORT", table="dbo.type_conversion_src" ❌
- INFORMATION_SCHEMA запрос идет к текущей БД (не к SUPPORT)
- Результат: колонки не найдены → fallback SELECT * → sql_variant не обрабатывается

**P0-2: SqlVariantQueryGenerator - отсутствие поддержки cross-database запросов**
- Текущий код: `SELECT ... FROM INFORMATION_SCHEMA.COLUMNS`
- Это всегда запрашивает ТЕКУЩУЮ БД (к которой подключен JdbcTemplate)
- Для запроса к другой БД нужно: `SELECT ... FROM SUPPORT.INFORMATION_SCHEMA.COLUMNS`

**P0-3: AvroToRecordTransformer - отсутствие precision/scale в ColumnMetadata**
- При создании метаданных из Avro схемы hardcoded precision=0, scale=0
- SQL Server bulk copy падает: "Length or precision specification 0 is invalid"

---

## Диагноз проблемы

### Проблема 1: Неправильный парсинг 3-частных имен таблиц (PRIMARY BLOCKER)

**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/SqlVariantQueryGenerator.java:56-59`

**Текущий код:**
```java
if (tableName.contains(".")) {
    String[] parts = tableName.split("\\.", 2);  // Разделяет только на ПЕРВОЙ точке
    schema = parts[0];  // "SUPPORT" ← НЕПРАВИЛЬНО (это database, не schema)
    table = parts[1];   // "dbo.type_conversion_src" ← НЕПРАВИЛЬНО
}
```

**Что происходит при tableName = "SUPPORT.dbo.type_conversion_src":**
1. Split на первой точке → ["SUPPORT", "dbo.type_conversion_src"]
2. schema = "SUPPORT" (на самом деле это database name)
3. table = "dbo.type_conversion_src" (на самом деле это schema.table)

**Последствия:**
- Неправильное понимание структуры имени таблицы
- INFORMATION_SCHEMA запрос ищет в неправильном месте (см. Проблему 2)

---

### Проблема 2: Отсутствие поддержки cross-database запросов (CRITICAL)

**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/SqlVariantQueryGenerator.java:127-132`

**Текущий код:**
```java
private List<ColumnInfo> queryInformationSchema(String schema, String table) {
    String query = """
            SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """;
```

**Проблема:**
- `FROM INFORMATION_SCHEMA.COLUMNS` - это shorthand для `FROM <current_database>.INFORMATION_SCHEMA.COLUMNS`
- JdbcTemplate подключен к определенной БД (возможно, не SUPPORT)
- Запрос всегда идет к **текущей** БД, а не к той, где находится таблица

**Пример:**
```sql
-- JdbcTemplate подключен к master или другой БД
-- Таблица находится в SUPPORT
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS  -- Ищет в master.INFORMATION_SCHEMA
WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'type_conversion_src'
-- Результат: 0 строк ❌
```

**Правильный запрос для cross-database:**
```sql
SELECT COLUMN_NAME, DATA_TYPE
FROM SUPPORT.INFORMATION_SCHEMA.COLUMNS  -- Явно указываем БД
WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'type_conversion_src'
-- Результат: все колонки таблицы ✅
```

**Каскадный эффект:**
1. `queryInformationSchema()` возвращает пустой список
2. Генерируется fallback `SELECT * FROM SUPPORT.dbo.type_conversion_src`
3. SQL_VARIANT_PROPERTY колонки не добавляются
4. JdbcStreamingResultSetExtractor не может определить sql_variant колонки
5. Колонка variant_col читается как raw Java тип (Integer, String)
6. TypeConverter падает: "Cannot convert Integer to Avro STRING"

---

### Проблема 3: Отсутствие precision/scale в ColumnMetadata из Avro

**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/transformer/AvroToRecordTransformer.java:68-75`

**Текущий код:**
```java
metadata.put(field.name(), new ColumnMetadata(
    field.name(),
    jdbcType,
    sqlTypeName,
    0, // precision - hardcoded 0 ← ПРОБЛЕМА
    0, // scale - hardcoded 0 ← ПРОБЛЕМА
    field.schema().isNullable()
));
```

**Что происходит:**
- При чтении из Kafka (Avro), ColumnMetadata создается без precision/scale
- Для DECIMAL полей это критично: DECIMAL(0,0) невалиден
- SQL Server bulk copy падает: "Length or precision specification 0 is invalid"

**Корректное поведение:**
- Для Avro decimal logical type нужно извлекать precision и scale из `LogicalTypes.Decimal`
- Для других типов можно оставить 0

---

## Решение

### Исправление 1: SqlVariantQueryGenerator - правильный парсинг 3-частных имен + cross-database поддержка

#### Требования:
Корректно обрабатывать имена таблиц во всех форматах:
- `"table"` → database=null, schema="dbo", table="table"
- `"schema.table"` → database=null, schema="schema", table="table"
- `"database.schema.table"` → database="database", schema="schema", table="table"

#### Архитектурное решение:

**1. Создать класс TableIdentifier для хранения parsed компонентов:**
```java
private record TableIdentifier(
    String database,  // nullable: если null - используется текущая БД
    String schema,
    String table,
    String fullName   // original name для использования в FROM clause
) {}
```

**2. Метод парсинга:**
```java
private TableIdentifier parseTableName(String tableName) {
    if (!tableName.contains(".")) {
        // Format: table
        return new TableIdentifier(null, "dbo", tableName, tableName);
    }

    String[] parts = tableName.split("\\.");
    if (parts.length == 2) {
        // Format: schema.table
        return new TableIdentifier(null, parts[0], parts[1], tableName);
    } else if (parts.length >= 3) {
        // Format: database.schema.table (or more parts - take last 3)
        String database = parts[parts.length - 3];
        String schema = parts[parts.length - 2];
        String table = parts[parts.length - 1];
        return new TableIdentifier(database, schema, table, tableName);
    }

    throw new IllegalArgumentException("Invalid table name format: " + tableName);
}
```

**3. Обновить метод queryInformationSchema для поддержки cross-database:**
```java
private List<ColumnInfo> queryInformationSchema(TableIdentifier tableId) {
    // Build INFORMATION_SCHEMA table reference
    String infoSchemaTable;
    if (tableId.database != null) {
        // Cross-database query: database.INFORMATION_SCHEMA.COLUMNS
        infoSchemaTable = tableId.database + ".INFORMATION_SCHEMA.COLUMNS";
    } else {
        // Same-database query: INFORMATION_SCHEMA.COLUMNS
        infoSchemaTable = "INFORMATION_SCHEMA.COLUMNS";
    }

    String query = String.format("""
            SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
            FROM %s
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """, infoSchemaTable);

    try {
        return jdbcTemplate.query(
                query,
                (rs, rowNum) -> new ColumnInfo(
                        rs.getString("COLUMN_NAME"),
                        rs.getString("DATA_TYPE"),
                        rs.getInt("ORDINAL_POSITION")
                ),
                tableId.schema,
                tableId.table
        );
    } catch (Exception e) {
        log.error("Failed to query INFORMATION_SCHEMA for table {}: {}",
                  tableId.fullName, e.getMessage(), e);
        throw new RuntimeException("Failed to generate SELECT query for table " + tableId.fullName, e);
    }
}
```

**4. Обновить generateSelectQuery:**
```java
public String generateSelectQuery(String tableName) {
    TableIdentifier tableId = parseTableName(tableName);

    log.debug("Generating SELECT query for table: database={}, schema={}, table={} (original={})",
              tableId.database, tableId.schema, tableId.table, tableId.fullName);

    List<ColumnInfo> columns = queryInformationSchema(tableId);

    if (columns.isEmpty()) {
        log.warn("No columns found for table {}, returning simple SELECT *", tableId.fullName);
        return "SELECT * FROM " + tableId.fullName;
    }

    // ... rest of the method remains the same
    // Use tableId.fullName in FROM clause
}
```

#### Примеры работы:

**Input:** `"type_conversion_src"`
- database=null, schema="dbo", table="type_conversion_src"
- Query: `FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='type_conversion_src'`
- SELECT: `SELECT ... FROM type_conversion_src`

**Input:** `"myschema.orders"`
- database=null, schema="myschema", table="orders"
- Query: `FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='myschema' AND TABLE_NAME='orders'`
- SELECT: `SELECT ... FROM myschema.orders`

**Input:** `"SUPPORT.dbo.type_conversion_src"` ✅
- database="SUPPORT", schema="dbo", table="type_conversion_src"
- Query: `FROM SUPPORT.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='type_conversion_src'`
- SELECT: `SELECT ... FROM SUPPORT.dbo.type_conversion_src`

---

### Исправление 2: AvroToRecordTransformer - извлечение precision/scale из Avro decimal

**Задача:** Для Avro decimal logical type извлекать precision и scale из схемы

**Реализация** (изменить метод `buildMetadataFromAvroSchema`, строки 60-79):

```java
private Map<String, ColumnMetadata> buildMetadataFromAvroSchema(Schema avroSchema) {
    Map<String, ColumnMetadata> metadata = new LinkedHashMap<>();

    for (Schema.Field field : avroSchema.getFields()) {
        Schema fieldSchema = unwrapNullable(field.schema());
        String sqlTypeName = avroTypeToSqlTypeName(fieldSchema);
        int jdbcType = sqlTypeNameToJdbcType(sqlTypeName);

        // Extract precision and scale for decimal types
        int precision = 0;
        int scale = 0;
        LogicalType logicalType = fieldSchema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Decimal decimalType) {
            precision = decimalType.getPrecision();
            scale = decimalType.getScale();
        }

        metadata.put(field.name(), new ColumnMetadata(
            field.name(),
            jdbcType,
            sqlTypeName,
            precision,  // Now extracts from Avro decimal
            scale,      // Now extracts from Avro decimal
            field.schema().isNullable()
        ));
    }

    return metadata;
}
```

**Обоснование:**
- Avro decimal хранит precision/scale в `LogicalTypes.Decimal`
- Без этих значений SQL Server не может создать корректный DECIMAL тип для bulk copy
- Для остальных типов precision/scale = 0 корректен (например, для INT, VARCHAR без указания длины)

---

## Критические файлы для изменения

### 1. SqlVariantQueryGenerator.java
**Путь:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/SqlVariantQueryGenerator.java`

**Изменения:**
- ✅ Добавить inner record `TableIdentifier` (после строки 40)
- ✅ Добавить метод `parseTableName(String)` (после строки 50)
- ✅ Обновить метод `generateSelectQuery(String)` (строки 51-117)
- ✅ Обновить метод `queryInformationSchema` - добавить поддержку cross-database (строки 119-149)
- ✅ Обновить JavaDoc класса - добавить информацию о поддержке 3-частных имен

### 2. AvroToRecordTransformer.java
**Путь:** `src/main/java/ru/pospelov/etl/engine/steps/transformer/AvroToRecordTransformer.java`

**Изменения:**
- ✅ Метод: `buildMetadataFromAvroSchema(Schema)` (строки 60-79)
- ✅ Добавить извлечение precision/scale из Avro decimal LogicalType

---

## План выполнения

### Шаг 1: Обновить SqlVariantQueryGenerator (15 минут)

1. Открыть `SqlVariantQueryGenerator.java`
2. Добавить inner record `TableIdentifier` (4 поля: database, schema, table, fullName)
3. Добавить метод `parseTableName(String tableName)` с поддержкой 1/2/3-частных имен
4. Обновить `generateSelectQuery`:
   - Вызвать `parseTableName` для парсинга имени
   - Передать `TableIdentifier` в `queryInformationSchema`
   - Использовать `tableId.fullName` в FROM clause
5. Обновить `queryInformationSchema`:
   - Изменить сигнатуру: `(TableIdentifier tableId)` вместо `(String schema, String table)`
   - Добавить логику выбора INFORMATION_SCHEMA таблицы (с database prefix или без)
   - Использовать `String.format` для построения query с dynamic table name
   - Обновить параметры jdbcTemplate.query: `tableId.schema, tableId.table`
6. Обновить JavaDoc класса
7. Обновить debug/error логи с новыми полями

### Шаг 2: Обновить AvroToRecordTransformer (5 минут)

1. Открыть `AvroToRecordTransformer.java`
2. Найти метод `buildMetadataFromAvroSchema` (строка ~60)
3. После получения `sqlTypeName` и `jdbcType` добавить:
   ```java
   int precision = 0;
   int scale = 0;
   LogicalType logicalType = fieldSchema.getLogicalType();
   if (logicalType instanceof LogicalTypes.Decimal decimalType) {
       precision = decimalType.getPrecision();
       scale = decimalType.getScale();
   }
   ```
4. Передать `precision` и `scale` в конструктор `ColumnMetadata`

### Шаг 3: Запустить тесты (2-3 минуты)

1. **Запустить TypeConversionIntegrationTest:**
   ```bash
   mvn clean test -Dtest=TypeConversionIntegrationTest
   ```

2. **Проверить успешное выполнение всех 6 тестов:**
   - `dateConversion_cornerCases_shouldPreserveExactValues`
   - `decimalConversion_precisionAndScale_shouldPreserveExactly`
   - `sqlVariantConversion_differentBaseTypes_shouldPreserveTypesAndValues`
   - `nullValues_allTypes_shouldPreserveNulls`
   - `binaryConversion_shouldPreserveExactBytes`
   - `uniqueidentifierConversion_shouldPreserveExactGuid`

3. **Проверить логи:**
   ```
   ✅ Должен появиться лог:
      "Generating SELECT query for table: database=SUPPORT, schema=dbo,
       table=type_conversion_src (original=SUPPORT.dbo.type_conversion_src)"

   ✅ Не должно быть предупреждений:
      "No columns found for table..."

   ✅ Не должно быть ошибок:
      "Length or precision specification 0 is invalid"
      "Cannot convert Integer to Avro STRING"
   ```

### Шаг 4: Проверить обратную совместимость (2-3 минуты)

Запустить все существующие интеграционные тесты для проверки, что изменения не сломали существующую функциональность:

```bash
mvn clean test -Dtest=KafkaToSqlIntegrationTest
mvn clean test -Dtest=EtlErrorHandlingTest
mvn clean test -Dtest=EtlPipelineCancellationTest
```

---

## Ожидаемый результат

### После всех исправлений:

✅ **SqlVariantQueryGenerator корректно парсит все форматы имен таблиц:**
- `"table"` → database=null, schema="dbo", table="table"
- `"schema.table"` → database=null, schema="schema", table="table"
- `"SUPPORT.dbo.type_conversion_src"` → database="SUPPORT", schema="dbo", table="type_conversion_src"

✅ **SqlVariantQueryGenerator поддерживает cross-database запросы:**
- Для таблиц из другой БД: `SELECT ... FROM SUPPORT.INFORMATION_SCHEMA.COLUMNS`
- Для таблиц из текущей БД: `SELECT ... FROM INFORMATION_SCHEMA.COLUMNS`
- INFORMATION_SCHEMA запрос возвращает все колонки
- SQL_VARIANT_PROPERTY колонки добавляются в генерируемый SELECT

✅ **AvroToRecordTransformer создает ColumnMetadata с правильным precision/scale:**
- Для DECIMAL(18,2): precision=18, scale=2
- Для DECIMAL(38,10): precision=38, scale=10
- SQL Server bulk copy успешно вставляет данные

✅ **Все 6 тестов TypeConversionIntegrationTest проходят:**
- `nullValues`: ✅ PASS
- `dateConversion`: ✅ PASS
- `decimalConversion`: ✅ PASS (сейчас падает на bulk insert)
- `binaryConversion`: ✅ PASS (сейчас падает на bulk insert)
- `guidConversion`: ✅ PASS
- `sqlVariantConversion`: ✅ PASS (сейчас падает на TypeConverter)

✅ **Обратная совместимость сохранена:**
- Существующие тесты (KafkaToSqlIntegrationTest и др.) продолжают работать
- 1-частные и 2-частные имена таблиц работают как прежде

---

## Риски и совместимость

### Уровень риска: **НИЗКИЙ**

**Причины:**
1. Изменения локализованы в двух методах двух классов
2. Не влияют на основной flow обработки данных
3. Обратная совместимость сохраняется:
   - Таблицы без database prefix работают как прежде
   - Fallback на текущую БД происходит автоматически (database=null)

### Потенциальные проблемы:

**1. SQL Injection риск** (MEDIUM)
- Используем `String.format` для добавления database prefix в SQL
- **Митигация:** database name берется из user-provided tableName, но:
  - Используется только в FROM clause INFORMATION_SCHEMA (read-only view)
  - Не используется в WHERE clause или других опасных местах
  - SQL Server сам валидирует имена объектов
- **Альтернатива (Stage 8):** Использовать prepared statements с quoted identifiers

**2. Permissions для cross-database доступа** (LOW)
- Требуется разрешение на SELECT к INFORMATION_SCHEMA в другой БД
- **Митигация:** В тестовой среде permissions уже есть (тесты используют SUPPORT БД)
- **Note:** Для production нужно обеспечить соответствующие permissions

**3. Совместимость с другими СУБД** (LOW)
- Изменения специфичны для SQL Server
- **Митигация:** Класс уже SQL Server-specific (использует SQL_VARIANT_PROPERTY)
- Для других СУБД можно добавить database-specific implementations (Stage 9)

### Обратная совместимость:

✅ **1-частные имена:** `"orders"` → работает как прежде (database=null, schema="dbo")
✅ **2-частные имена:** `"dbo.orders"` → работает как прежде (database=null)
✅ **3-частные имена:** `"SUPPORT.dbo.orders"` → **НОВАЯ ФУНКЦИОНАЛЬНОСТЬ**

### Тестовое покрытие:

После реализации рекомендуется добавить unit-тесты для `SqlVariantQueryGenerator`:
1. Test parseTableName с 1/2/3-частными именами
2. Test generateSelectQuery с разными форматами имен
3. Test queryInformationSchema с cross-database table

---

## Оценка времени

- **Исправление SqlVariantQueryGenerator:** 15 минут
- **Исправление AvroToRecordTransformer:** 5 минут
- **Запуск и проверка тестов:** 3-5 минут
- **Проверка обратной совместимости:** 2-3 минут
- **Итого:** ~25-30 минут

---

## Дополнительные наблюдения

### Тесты, которые уже проходят (из логов):

1. `nullValues_allTypes_shouldPreserveNulls` - ✅ PASS
2. `dateConversion_cornerCases_shouldPreserveExactValues` - ✅ PASS
3. `uniqueidentifierConversion_shouldPreserveExactGuid` - ✅ PASS

### Тесты, которые падают:

1. `decimalConversion_precisionAndScale_shouldPreserveExactly` - ❌ "Length or precision specification 0 is invalid"
2. `binaryConversion_shouldPreserveExactBytes` - ❌ "Length or precision specification 0 is invalid"
3. `sqlVariantConversion_differentBaseTypes_shouldPreserveTypesAndValues` - ❌ "Cannot convert Integer to Avro STRING"

### Почему одни тесты проходят, а другие нет?

**Проходят:**
- Используют простые типы (DATE, UNIQUEIDENTIFIER, NULL), которые не требуют precision/scale
- Не используют sql_variant колонки

**Падают:**
- `decimalConversion`: требует precision/scale для DECIMAL типов
- `binaryConversion`: вероятно, также требует metadata для VARBINARY
- `sqlVariantConversion`: требует корректной обработки sql_variant → SqlVariantValue

---

## Проверка после исправления

### 1. Запустить TypeConversionIntegrationTest
Все 6 тестов должны пройти

### 2. Проверить логи
```
✅ "Generating SELECT query for table: database=SUPPORT, schema=dbo,
    table=type_conversion_src (original=SUPPORT.dbo.type_conversion_src)"

✅ "Generated SQL query for table SUPPORT.dbo.type_conversion_src with N sql_variant columns"

❌ Не должно быть: "No columns found for table..."
❌ Не должно быть: "Length or precision specification 0 is invalid"
❌ Не должно быть: "Cannot convert Integer to Avro STRING"
```

### 3. Запустить остальные интеграционные тесты
Проверить обратную совместимость:
- `KafkaToSqlIntegrationTest` - должен продолжать работать
- `EtlErrorHandlingTest` - должен продолжать работать
- `EtlPipelineCancellationTest` - должен продолжать работать

---

## Следующие шаги (после Stage 4)

### Stage 5: Unit-тесты для SqlVariantQueryGenerator
- Тестировать `parseTableName` с различными форматами
- Тестировать `generateSelectQuery` с mock JdbcTemplate

### Stage 6: Обработка edge cases
- Таблицы с спецсимволами в именах (brackets, quotes)
- Таблицы с >3 точками в имени
- Case sensitivity (SQL Server case-insensitive by default, но может быть изменено)

### Stage 7: Performance optimization
- Кэширование результатов INFORMATION_SCHEMA запросов
- Batch запросы для нескольких таблиц

### Stage 8: Security hardening
- Использовать quoted identifiers для database/schema/table names
- Добавить validation для table names

### Stage 9: Multi-database support
- Поддержка других СУБД (PostgreSQL, MySQL, Oracle)
- Database-specific query generators

---

**Документ создан:** 2025-12-18
**Статус:** Ready for implementation
**Приоритет:** P0 (блокирующие тесты)
