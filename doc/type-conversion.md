# Конвертация типов SQL Server <-> Avro (и обратно)

Документ фиксирует правила соответствия типов между **SQL Server**, **Java (канонический тип внутри pipeline)** и **Avro (Confluent Schema Registry)**, а также список доработок, которые нужны проекту для корректной типизации (включая `DATE/datetime2` и `sql_variant`).

Контекст проекта (почему тема важна):
- Экстрактор JDBC читает значения через `ResultSet.getObject()` без нормализации типов: `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcStreamingResultSetExtractor.java`.
- Преобразование "плоские колонки <-> Avro" реализовано через связку transformer'ов и loader'ов:
  - `RecordToAvroTransformer.java` подготавливает данные для Avro
  - `KafkaByPartitionLoader` выполняет конвертацию Java → Avro используя TypeConverter
  - `AvroToRecordTransformer.java` разворачивает Avro GenericRecord и передает метаданные схемы для корректной конвертации в SQL loader'ах
- SQL loader'ы используют `ps.setObject(...)` и/или bulk copy; тип для bulk copy выводится из Java-типа значения после конвертации через TypeConverter: `src/main/java/ru/pospelov/etl/engine/model/EtlBulkRecord.java`.
- **Ключевой момент:** `AvroToRecordTransformer` создает `ColumnMetadata` из Avro schema, маппируя Avro logical types на SQL типы. Это позволяет TypeConverter корректно конвертировать Avro физические типы (например, `int` для date) в SQL-совместимые Java типы (`LocalDate`).

Если **Java-тип, пришедший из JDBC**, не совпадает с **Avro физическим типом схемы** (или с ожиданиями SQL при вставке), то возникают:
- падения сериализации Avro (ошибки типа/union),
- потери семантики (timezone, scale/precision),
- ошибки вставки в SQL.

---

## 1) Соотношение типов и аргументация

### 1.0. Где должна жить конвертация (архитектурное решение)

Цель: иметь **однозначный контракт типов** и при этом конвертировать значения **на лету**, не добавляя отдельной материализации данных в памяти.

Принятое решение для проекта:
- **Нормализация JDBC -> канонический Java тип** делается максимально рано (на входе из JDBC), чтобы дальше по pipeline типы были стабильными:
  - `java.sql.Date` → `LocalDate`
  - `java.sql.Timestamp` → `Instant` (UTC)
  - `java.sql.Time` → `LocalTime`
  - Driver-specific numeric → `BigDecimal` (для DECIMAL/NUMERIC/MONEY)
- **Конвертация под конкретный sink** делается в **loader'ах**, потому что loader знает целевой контракт:
  - для Kafka/Avro: Avro schema (`physical type`/union/`logicalType`);
  - для SQL: ожидаемые SQL-типы таблицы и правила bind'а (`PreparedStatement`/bulk copy).

Практическое следствие:
- Transformer'ы `avro` и `record-to-avro` становятся временным слоем совместимости; целевое состояние — `noop` + "умные" loader'ы.
- Канонизация типов гарантирует предсказуемость поведения на всех этапах pipeline.

### 1.1. Принципы (почему так "правильно")

1) **Avro строго проверяет физические типы.**  
`logicalType` не является автоматической конвертацией; значение в `GenericRecord` должно соответствовать физическому типу схемы (`int/long/bytes/...`).

2) **JDBC `getObject()` не гарантирует единственный Java-тип.**  
Фактический тип зависит от драйвера, настроек и типа колонки. Поэтому проекту нужен слой приведения к каноническим типам.

3) **`DATE`, `TIME`, `datetime2` - разные сущности.**  
`DATE` - календарная дата без времени и без timezone; `TIME` - время суток; `datetime2` - дата-время без timezone. В Avro это должны быть разные физические типы/`logicalType`, иначе теряется семантика или появляется двусмысленность.

4) **`DECIMAL/NUMERIC/MONEY` должны передаваться без потери точности.**  
Для точных чисел нужна модель `BigDecimal` в Java и `decimal` logicalType в Avro (на базе `bytes`), иначе неизбежны округления.

5) **`sql_variant` - контейнер разных типов и не может быть одним Avro-типом.**  
Чтобы сохранить однозначность, значение переносится в текстовом конверте вместе с точным именем SQL Server типа.

### 1.2. Однозначная матрица типов (SQL Server -> Java -> Avro)

Таблица задаёт **канонический контракт** для каждого типа SQL Server:
- какой **Java тип** используется внутри pipeline,
- какой **Avro физический тип** и `logicalType` используется в схеме.

Общие правила:
- Nullable в SQL отражается в Avro через union `["null", <type>]` + `default: null`.
- Параметры `precision/scale/length` берутся из DDL/метаданных колонки.

| SQL Server тип | Канонический Java тип | Avro физ. тип | Avro logicalType | Почему (общая аргументация) |
|---|---|---:|---|---|
| `BIT` | `Boolean` | `boolean` | - | Булево значение без потерь и двусмысленности. |
| `TINYINT` | `Integer` | `int` | - | Единый целочисленный контракт малого диапазона, совместимый с Avro `int`. |
| `SMALLINT` | `Integer` | `int` | - | Единый целочисленный контракт малого диапазона, совместимый с Avro `int`. |
| `INT` | `Integer` | `int` | - | Прямое соответствие 32-bit signed. |
| `BIGINT` | `Long` | `long` | - | Прямое соответствие 64-bit signed. |
| `REAL` | `Float` | `float` | - | Соответствие одинарной точности. |
| `FLOAT` | `Double` | `double` | - | Соответствие двойной точности. |
| `DECIMAL(p,s)` | `BigDecimal` | `bytes` | `decimal` | Сохранение точности и scale/precision без округлений. |
| `NUMERIC(p,s)` | `BigDecimal` | `bytes` | `decimal` | Сохранение точности и scale/precision без округлений. |
| `MONEY` | `BigDecimal` | `bytes` | `decimal` | Сохранение точности для финансовых значений. |
| `SMALLMONEY` | `BigDecimal` | `bytes` | `decimal` | Сохранение точности для финансовых значений. |
| `CHAR(n)` | `String` | `string` | - | Единый текстовый контракт для межсистемного обмена. |
| `VARCHAR(n)` | `String` | `string` | - | Единый текстовый контракт для межсистемного обмена. |
| `VARCHAR(MAX)` | `String` | `string` | - | Единый текстовый контракт для межсистемного обмена. |
| `TEXT` | `String` | `string` | - | Единый текстовый контракт для межсистемного обмена. |
| `NCHAR(n)` | `String` | `string` | - | Unicode-текст в строковом контракте. |
| `NVARCHAR(n)` | `String` | `string` | - | Unicode-текст в строковом контракте. |
| `NVARCHAR(MAX)` | `String` | `string` | - | Unicode-текст в строковом контракте. |
| `NTEXT` | `String` | `string` | - | Unicode-текст в строковом контракте. |
| `UNIQUEIDENTIFIER` | `String` | `string` | `uuid` | Переносимый контракт идентификатора между системами. |
| `BINARY(n)` | `byte[]` | `bytes` | - | Бинарные данные без перекодирования и потери содержимого. |
| `VARBINARY(n)` | `byte[]` | `bytes` | - | Бинарные данные без перекодирования и потери содержимого. |
| `VARBINARY(MAX)` | `byte[]` | `bytes` | - | Бинарные данные без перекодирования и потери содержимого. |
| `IMAGE` | `byte[]` | `bytes` | - | Бинарные данные без перекодирования и потери содержимого. |
| `ROWVERSION` (alias `TIMESTAMP`) | `byte[]` | `bytes` | - | Техническое бинарное значение фиксированной длины. |
| `XML` | `String` | `string` | - | Текстовый контракт для структурированных документов. |
| `DATE` | `LocalDate` | `int` | `date` | Календарная дата без времени и без timezone. |
| `TIME(p)` | `LocalTime` | `long` | `time-micros` | Время суток без даты и без timezone в числовом виде. |
| `SMALLDATETIME` | `Instant` | `long` | `timestamp-millis` | Унификация даты-времени в транспортной модели "момент времени". |
| `DATETIME` | `Instant` | `long` | `timestamp-millis` | Унификация даты-времени в транспортной модели "момент времени". |
| `DATETIME2(p)` | `Instant` | `long` | `timestamp-millis` | Унификация даты-времени в транспортной модели "момент времени". |
| `DATETIMEOFFSET(p)` | `OffsetDateTime` | `string` | - | Сохранение offset, который не выражается Avro timestamp logicalType. |
| `GEOGRAPHY` | `String` | `string` | - | Универсальный переносимый текстовый контракт. |
| `GEOMETRY` | `String` | `string` | - | Универсальный переносимый текстовый контракт. |
| `HIERARCHYID` | `String` | `string` | - | Универсальный переносимый текстовый контракт. |
| `SQL_VARIANT` | `SqlVariantValue` | `string` | - | Значение переносится в текстовом конверте, тип хранится внутри конверта для восстановления при обратной записи. |

**Важно:** Таблица описывает **канонические** типы. В реальности JDBC драйвер может возвращать legacy типы (`java.sql.Date`, `java.sql.Timestamp`, `java.sql.Time`). TypeConverter автоматически конвертирует их в канонические:
- `java.sql.Date` → `LocalDate` (при чтении из JDBC) → `int` (Avro date)
- `java.sql.Timestamp` → `Instant` (при чтении из JDBC) → `long` (Avro timestamp-millis)
- `java.sql.Time` → `LocalTime` (при чтении из JDBC) → `long` (Avro time-micros)
- `String` (если драйвер возвращает строку для даты) → парсится в соответствующий тип согласно Avro schema

### 1.3. Стратегия для `DATE` и `datetime2` (ключевой момент)

Фиксируем решения для проекта:
- SQL `DATE` -> Java `LocalDate` -> Avro `int` + `logicalType: "date"`.
- SQL `DATETIME2(p)` -> Java `Instant` -> Avro `long` + `logicalType: "timestamp-millis"`.
- Временная шкала для `datetime2` трактуется как **UTC** (пока без усложнений).

### 1.4. Стратегия для `sql_variant`

Фиксируем решения для проекта:
- В Java используем отдельный тип `SqlVariantValue`, который хранит:
  - `sqlType` - **точно как в SQL Server** (например `int`, `decimal(18,2)`, `datetime2(7)`, `varbinary(max)`),
  - `value` - каноническое строковое представление,
  - `encoding` - `plain|base64|hex` (для бинарных/по соглашению).
- В Avro поле остаётся `string`; внутри хранится JSON-конверт одной строкой (без вложенных схем).
- При обратной загрузке в SQL остаётся **одна** колонка `sql_variant`, и внутри неё должен сохраниться правильный базовый тип.

Пример формата JSON-конверта:
- `{"v":1,"t":"decimal(18,2)","val":"12.30","enc":"plain"}`

Откуда брать `sqlType`:
- приоритетно - из SQL Server (если доступно на источнике),
- допускается - выводить из Java-класса значения (проще, но хуже для параметризованных типов вроде `decimal(p,s)`).

---

## 2) Предлагаемые доработки в данном проекте

### 2.1. Конвертация типов в loader'ах + отдельный конвертер

Архитектура конвертации типов в проекте основана на разделении ответственности:

**TypeConverter** - централизованный класс конвертации:
- Не зависит от инфраструктуры Kafka/SQL/Avro
- Содержит чистую логику преобразований типов
- Вызывается из loader'ов в момент записи данных
- Поддерживает канонические типы (LocalDate, Instant) и legacy JDBC типы (java.sql.Date, java.sql.Timestamp)
- Может парсить строковые даты для совместимости с различными JDBC драйверами

**Kafka loader (format=AVRO)**:
- Получает данные с каноническими Java типами (LocalDate, Instant, BigDecimal)
- Через TypeConverter конвертирует в Avro физические типы (int для date, long для timestamp-millis, ByteBuffer для decimal)
- Собирает GenericRecord по Avro schema
- Отправляет в Kafka

**SQL loader'ы (JDBC/bulk copy)**:
- Получают данные либо с Avro физическими типами (Integer для date), либо каноническими Java типами
- Через TypeConverter конвертируют в JDBC-совместимые типы используя метаданные колонок
- Выполняют вставку через PreparedStatement или BulkCopy

**Ключевой механизм: передача метаданных через EtlBatch**

`EtlBatch` содержит:
- `Collection<EtlRecord> records` - данные
- `Map<String, ColumnMetadata> columnMetadata` - метаданные типов колонок

Источники метаданных:
- **JDBC Extractor**: создает `ColumnMetadata` из `ResultSetMetaData`, исключая служебные `__variant_*` колонки
- **AvroToRecordTransformer**: создает `ColumnMetadata` из Avro schema, маппируя logical types на SQL типы (например, `logicalType: "date"` → `typeName: "date"`)
- **Kafka Extractor**: создает batch с `columnMetadata = null` (метаданные отсутствуют при чтении бинарных данных)

Без метаданных TypeConverter не может корректно конвертировать Avro физические типы (например, отличить "просто Integer" от "Avro date (int epoch days)").

**Контракт EtlRecord**:
- `Map<String, Object> fields` - только пользовательские колонки/поля данных
- Служебные поля Kafka envelope: `__kafka_key`, `__kafka_value` (для разделения от пользовательских колонок `key`/`value`)
- Служебные колонки `__variant_*` НЕ включаются в `fields` (используются только внутри JDBC extractor для чтения метаданных sql_variant)
- SQL loader'ы используют только пользовательские поля для формирования INSERT (фильтруют `__kafka_*` и `__variant_*`)

`key/value` для Kafka (важный конфликт имён):
- Проблема: в текущей реализации проекта используются служебные ключи `key` и `value` для Kafka envelope, что конфликтует с реальными SQL-колонками `key`/`value`.
- Принятое решение: **переименовать служебные поля Kafka envelope в `__kafka_key` и `__kafka_value`**, чтобы `key`/`value` могли быть обычными колонками без перезаписи.

### 2.2. Обработка `sql_variant`

**Представление в pipeline:**
- В Java: `SqlVariantValue` (содержит sqlType, value, encoding)
- В Avro: `string` (JSON-конверт вида `{"v":1,"t":"decimal(18,2)","val":"12.30","enc":"plain"}`)
- При вставке в SQL: распаковывается в базовый JDBC-совместимый тип

**Обнаружение sql_variant колонок:**

JDBC Extractor обнаруживает sql_variant колонки по наличию вспомогательных `__variant_*` колонок в `ResultSetMetaData`:
- Для каждой sql_variant колонки `vcol` автогенерация SQL добавляет колонки:
  - `__variant_vcol_basetype`
  - `__variant_vcol_precision`
  - `__variant_vcol_scale`
  - `__variant_vcol_maxlength`
- Эти колонки используются только для чтения метаданных и **НЕ включаются** в `ColumnMetadata` или `EtlRecord.fields`

**Важно:** базовый тип `sql_variant` может отличаться **в каждой строке** (в одной строке `int`, в другой `nvarchar`). `ResultSetMetaData` сообщает только что колонка имеет тип `sql_variant`, но не сообщает базовый тип конкретного значения. Поэтому `sqlType` для `SqlVariantValue` читается из `__variant_*` колонок **для каждой строки отдельно**.

Рекомендуемый способ получить `sqlType` "точно как в SQL Server":
- добавить в SQL-запрос вычисляемые колонки через `SQL_VARIANT_PROPERTY` для каждого `sql_variant`-поля, например:
  - `SQL_VARIANT_PROPERTY(vcol, 'BaseType')` (базовый тип),
  - `SQL_VARIANT_PROPERTY(vcol, 'Precision')`, `SQL_VARIANT_PROPERTY(vcol, 'Scale')` (для numeric/decimal и др.),
  - `SQL_VARIANT_PROPERTY(vcol, 'MaxLength')` (для строк/байтов),
  - `SQL_VARIANT_PROPERTY(vcol, 'Collation')` (если нужно различать колляции/строковые типы).
- на основе этих свойств собирать строку `sqlType` (например `decimal(18,2)`, `datetime2(7)`, `varbinary(16)` и т.п.) и класть её в конверт.

Допустимый упрощённый способ:
- выводить `sqlType` из Java-класса, который вернул драйвер (`Integer` -> `int`, `BigDecimal` -> `decimal(p,s)` по `precision/scale`, `String` -> строковый тип по соглашению и т.д.).
Минус: для строковых и параметризованных типов это может быть неточно (например `varchar` vs `nvarchar`, точные параметры, колляция).

Требования к конвертации:
- Kafka loader (format=AVRO): если в `data` лежит `SqlVariantValue`, сериализовать его в JSON-строку и положить в Avro `string`.
- SQL loader (JDBC/bulk): перед bind/insert `SqlVariantValue` преобразовать в "реальный" JDBC-совместимый Java-тип базового значения (`Integer`, `Long`, `BigDecimal`, `Boolean`, `byte[]`, `java.sql.Date`, `Timestamp`, ...), так чтобы SQL Server сохранил правильный базовый тип внутри `sql_variant`.

### 2.3. Bulk copy: типизация и фильтрация полей

**EtlBulkRecord.mapJavaToSqlType** поддерживает полный набор типов:

**Java time типы** (канонические):
- `LocalDate` → `Types.DATE`
- `Instant` → `Types.TIMESTAMP`
- `LocalTime` → `Types.TIME`
- `OffsetDateTime` → `Types.TIMESTAMP_WITH_TIMEZONE`

**Legacy JDBC типы**:
- `java.sql.Date` → `Types.DATE`
- `java.sql.Timestamp` → `Types.TIMESTAMP`
- `java.sql.Time` → `Types.TIME`

**Числовые типы**:
- `Integer` → `Types.INTEGER`
- `Long` → `Types.BIGINT`
- `BigDecimal` → `Types.DECIMAL`
- `Float` → `Types.FLOAT`
- `Double` → `Types.DOUBLE`

**Прочие типы**:
- `Boolean` → `Types.BOOLEAN`
- `byte[]` → `Types.VARBINARY`
- `String` → `Types.VARCHAR` (по умолчанию)

### 2.4. Конвертация Avro logical types

**Направление SQL → Kafka (Avro):**

TypeConverter в Kafka loader конвертирует канонические Java типы в Avro физические типы:
- `LocalDate` → `Integer` (epoch days) для `logicalType: "date"`
- `Instant` → `Long` (epoch millis) для `logicalType: "timestamp-millis"`
- `LocalTime` → `Long` (microseconds) для `logicalType: "time-micros"`
- `BigDecimal` → `ByteBuffer` (Avro decimal encoding) для `logicalType: "decimal"`

Также поддерживаются legacy JDBC типы и строковые даты (парсятся в соответствующий тип).

**Направление Kafka (Avro) → SQL:**

AvroToRecordTransformer создает ColumnMetadata из Avro schema, маппируя logical types на SQL типы. TypeConverter использует эти метаданные для обратной конвертации:
- `Integer` (Avro date) + `metadata.typeName = "date"` → `LocalDate`
- `Long` (Avro timestamp-millis) + `metadata.typeName = "datetime2"` → `Instant`
- `ByteBuffer` (Avro decimal) + metadata.scale → `BigDecimal`

### 2.5. Bulk copy: фильтрация полей и column mapping

**FastSqlServerLoader** выполняет следующую обработку:

1. **Конвертация типов через TypeConverter**:
   - Для каждого поля в записи вызывается `TypeConverter.convertToJdbc()`
   - Avro физические типы (Integer для date) конвертируются в канонические Java типы (LocalDate)
   - Канонические Java типы конвертируются в JDBC-совместимые типы (java.sql.Date)
   - Используется ColumnMetadata для корректной интерпретации типов

2. **Фильтрация служебных полей**:
   - Из `EtlRecord.getAll()` исключаются поля с префиксами:
     - `__kafka_*` (Kafka envelope: `__kafka_key`, `__kafka_value`)
     - `__variant_*` (вспомогательные колонки для sql_variant метаданных)
   - Только пользовательские колонки участвуют в `addColumnMapping` и вставке

3. **Column mapping**:
   - Для каждой пользовательской колонки вызывается `bulkCopy.addColumnMapping(columnName, columnName)`
   - Гарантируется соответствие порядка колонок между источником и целевой таблицей

4. **Защита от ошибок конфигурации**:
   - Если в отфильтрованных данных есть колонки, которых нет в целевой таблице, bulk copy операция завершится ошибкой SQL Server
   - Это предотвращает случайную вставку данных в неправильные колонки

### 2.6. Конвертация типов в Kafka loader (format=AVRO)

**KafkaByPartitionLoader** с format=AVRO:

1. **Работа с GenericRecord из RecordToAvroTransformer**:
   - Если `__kafka_value` содержит `GenericRecord` (путь через RecordToAvroTransformer)
   - Перестраивает его с применением TypeConverter для корректной конвертации типов
   - Конвертирует каждое поле согласно Avro schema (LocalDate → int, Instant → long, etc.)

2. **Конвертация типов**:
   - Для каждого поля GenericRecord вызывается `TypeConverter.convertToAvro(value, field, jobId)`
   - Канонические Java типы конвертируются в Avro физические типы
   - Legacy JDBC типы (java.sql.Date, java.sql.Timestamp) также поддерживаются
   - Строковые даты парсятся при необходимости

3. **SqlVariantValue**:
   - Конвертируется в JSON-строку через `TypeConverter.sqlVariantToJson()`
   - Хранится в Avro как обычное string поле

4. **Отправка в Kafka**:
   - Формируется `ProducerRecord` с ключом из `__kafka_key`
   - GenericRecord отправляется как value
   - Avro serializer автоматически проверяет соответствие типов схеме

### 2.7. Интеграционные тесты с корректными SQL типами

Интеграционные тесты используют корректные SQL типы для проверки конвертации:
- `order_date DATE` - тестирование конвертации date логического типа
- `delivery_date DATE NULL` - тестирование nullable date полей
- Timestamp поля используют `DATETIME2` для тестирования timestamp-millis

---

## 3) Логи/диагностика несовместимости типов (на границах: loader/encoder)

Принцип:
- Детальные логи несовместимости типов пишем там, где известен **целевой контракт**:
  - Kafka loader (Avro): Avro schema (physical type/union/`logicalType`);
  - SQL loader (JDBC/bulk): ожидаемый SQL тип (из конфигурации/DDL/метаданных).
- В экстракторах оставляем обычные ошибки выполнения без type-debug, чтобы не размазывать первопричину по слоям.

### 3.1. Логи SQL->Avro (Kafka loader, format=AVRO)

При ошибке приведения/записи поля в `GenericRecord` логировать:
- `fieldName`
- `schemaExpected`: тип/union/`logicalType`
- `actualJavaType`: `value.getClass().getName()` (или `null`)
- `valuePreview` (без больших дампов)
- `job/partition/offset/key` (если есть в `EtlRecord`)
- `sourceSqlType` (если метаданные SQL доступны из контекста, см. 3.3)

### 3.2. Логи Avro->SQL (SQL loader)

Конвертацию Avro->SQL-совместимые Java-типы делаем в loader'е (до bind/insert). При ошибке конвертации логировать:
- `fieldName`
- Avro schema info: тип/union/`logicalType`
- `actualJavaType` и `valuePreview`
- `expectedSqlType` (если введёте явное маппирование "поле -> SQL тип")
- `job/partition/offset/key`

### 3.3. Метаданные JDBC (без логирования) для обогащения ошибок loader'ов

В `JdbcStreamingResultSetExtractor` можно собирать метаданные по колонкам (без логов):
- `md.getColumnType(i)`, `md.getColumnTypeName(i)`, `md.getPrecision(i)`, `md.getScale(i)`

И хранить их в метаданных batch/партиции (один объект на batch) или как shared reference, чтобы loader мог вывести `sourceSqlType`, не дублируя per-record map.

### 3.4. Формат сообщения об ошибке

Рекомендуемый формат одной строкой:
`Type mismatch at SQL->Avro: field=... expected=... actual=... sqlType=... job=... partition=... offset=...`
