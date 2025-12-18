# Отчет проверки реализации конвертации типов (SQL Server <-> Avro)

Дата: 2025-12-17

Проверены документы:
- `doc/implementation-plan.md`
- `doc/type-conversion-requirements.md`
- `doc/type-conversion.md`

Проверена реализация и тесты (статический аудит кода; тесты не запускались в рамках этого отчета).

## Краткий итог

Реализация закрывает часть требований по структуре pipeline и поддержке `sql_variant` (включая переименование Kafka envelope в `__kafka_*`, введение `EtlBatch`, модель `SqlVariantValue`, генерацию/валидацию `__variant_*`).

Ключевые требования по **типобезопасной** конвертации `DATE`/`DATETIME2`/`DECIMAL` **не обеспечены end-to-end**:
- JDBC extractor не приводит значения к каноническим Java-типам (`LocalDate`/`Instant`/`BigDecimal`), а кладет в `EtlRecord` результат `ResultSet.getObject()`.
- `TypeConverter.convertToJdbc(...)` не конвертирует Avro физические типы (например, `int` для `date`, `long` для `timestamp-millis`, `ByteBuffer` для `decimal`) в JDBC-совместимые значения.
- В итоге направление Kafka(Avro)->SQL для логических типов Avro сейчас выглядит неработоспособным без дополнительных преобразований.

Дополнительно: пример Avro-схемы `src/main/resources/avro/order-events-value.avsc` до сих пор использует `string` для дат и `double` для сумм, что расходится с целевой матрицей типов из документов.

## Статус требований (сводно)

| Раздел требований | Статус | Комментарий / проверка в коде |
|---|---|---|
| 2.1 Kafka envelope `__kafka_key`/`__kafka_value` | OK | `KafkaPartitionExtractor`, `KafkaByPartitionLoader`, `RecordToAvroTransformer`, `AvroToRecordTransformer`, `JdbcStreamingResultSetExtractor` работают с `__kafka_*`. |
| 2.2 Контракт `EtlRecord` | PARTIAL | Структура соответствует, но в тестах есть устаревшее использование `key/value` как служебных ключей (`src/test/java/ru/pospelov/etl/engine/model/EtlBatchTest.java`). |
| 2.3 Контракт `EtlBatch` | PARTIAL | `EtlBatch` внедрен и используется в pipeline (`EtlComponentFactory` и шаги). При этом остался неиспользуемый legacy-интерфейс `src/main/java/ru/pospelov/etl/engine/steps/extractor/Extractor.java` с `Consumer<Collection<EtlRecord>>`. |
| 2.4 `ColumnMetadata` | OK/PARTIAL | Класс реализован. В `JdbcStreamingResultSetExtractor` метаданные собираются «как есть» из `ResultSetMetaData`, включая `__variant_*` колонки. |
| 3.1 SQL `DATE` (канонический `LocalDate`, Avro `int`+`date`) | FAIL | Нет нормализации в JDBC extractor (ожидаемо будет `java.sql.Date`), `TypeConverter.convertToAvro` не поддерживает `java.sql.Date`; обратная конвертация Avro `int`->`Date`/`LocalDate` отсутствует. |
| 3.2 SQL `DATETIME2(p)` (канонический `Instant`, Avro `long`+`timestamp-millis`) | FAIL | Аналогично: JDBC обычно дает `Timestamp`, а `convertToAvro` его не обрабатывает; обратная конвертация Avro `long`->`Timestamp` отсутствует. |
| 3.3 DECIMAL/NUMERIC/MONEY (`BigDecimal`, Avro decimal bytes) | FAIL | `convertToAvro(BigDecimal)` реализован, но: (1) в JDBC extractor нет гарантии канонического `BigDecimal` vs driver особенностей; (2) `convertToJdbc(ByteBuffer)` не восстанавливает `BigDecimal`; (3) пример Avro schema использует `double`. |
| 4.x `sql_variant` (модель/транспорт/восстановление) | PARTIAL | `SqlVariantValue` + JSON-конверт реализованы; table-based генерация и custom-query валидация `__variant_*` есть. Отклонение от требований: обратная сторона не создает `SqlVariantValue` в extractor’е Kafka — вместо этого SQL loader распаковывает JSON строку напрямую. |
| 5.x Где выполняется конвертация (`TypeConverter`, loader-границы) | PARTIAL | Архитектурно конвертация вынесена в loader’ы, но отсутствуют критичные правила Avro->JDBC для logical types и JDBC->каноника. |
| 5.5 Bulk copy: фильтрация служебных полей | OK | В `FastSqlServerLoader`/`JdbcLoader` фильтруются `__kafka_*` и `__variant_*` перед вставкой. |
| 6 Логи/диагностика несовместимости типов | PARTIAL | Есть логи в loader’ах, но не выполняется требование полноты контекста (job/partition/offset/key) и «ожидаемый тип» не везде формируется. |
| 7 Тесты | PARTIAL | Unit-тесты есть, но есть пробелы и некорректный тест; интеграционные тесты завязаны на внешний SQL Server/Kafka/Schema Registry и по умолчанию исключены surefire’ом. |
| 8 Breaking changes (переход на `EtlBatch`) | PARTIAL | Фактический pipeline на `EtlBatch`, но остался legacy `Extractor` интерфейс со старой сигнатурой. |

## Противоречия и замечания

### В документах
- `doc/type-conversion-requirements.md` в разделе 4.4 одновременно:
  - говорит «собрать ColumnMetadata для всех колонок (включая `__variant_*`)»,
  - и ниже требует «`__variant_*` НЕ включаются в ColumnMetadata batch’а».
  Это внутреннее противоречие требований; реализация сейчас ближе к варианту «включать в ColumnMetadata».

### В коде
- `src/main/java/ru/pospelov/etl/engine/conversion/TypeConverter.java`: Javadoc для `convertToJdbc` декларирует конверсию Avro logical types (`int` date, `long` timestamp, `ByteBuffer` decimal), но фактически этого нет.
- `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcExtractor.java`: `addWhereClause(...)` через JSqlParser приводит `select.getSelectBody()` к `PlainSelect`. Для `UNION`, `WITH`, и других неплоских `SELECT` это может завершиться `ClassCastException` и сорвать валидацию custom query.

## Проверка тестов

### Что есть
- Unit-тесты конвертера: `src/test/java/ru/pospelov/etl/engine/conversion/TypeConverterTest.java`.
- Unit-тесты моделей: `ColumnMetadataTest`, `EtlBatchTest`, `SqlVariantValueTest`, `EtlBulkRecordTest`.
- Проверка валидации sql_variant для custom query: `src/test/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcExtractorValidationTest.java`.
- Интеграционные тесты round-trip: `src/test/java/ru/pospelov/etl/engine/TypeConversionIntegrationTest.java`, `src/test/java/ru/pospelov/etl/engine/KafkaToSqlIntegrationTest.java` (по умолчанию исключены Maven Surefire по маске `*IntegrationTest.java`).

### Проблемы/пробелы
- В `TypeConverterTest` тест `testFullPipeline_SqlToKafkaToSql_Date` логически неверный: на шаге «SQL Loader» он вызывает `convertToJdbc(originalDate, ...)`, а должен проверять обратную конвертацию результата Avro (`Integer` epochDays) — иначе тест не ловит реальную ошибку.
- Нет unit-тестов на:
  - `convertToJdbc(Integer epochDays -> java.sql.Date/LocalDate)`
  - `convertToJdbc(Long epochMillis -> java.sql.Timestamp/Instant)`
  - `convertToJdbc(ByteBuffer decimal -> BigDecimal)`
  - `convertToAvro(java.sql.Date/java.sql.Timestamp -> Avro)` (что важно, т.к. JDBC extractor сейчас использует `getObject()`)
- Нет теста на требование «отсутствие конфликта имен»: таблица с реальными колонками `key` и `value` должна корректно проходить pipeline после переименования Kafka envelope.
- `JdbcExtractorValidationTest` помечен `@SpringBootTest`, но не исключен surefire’ом (имя не `*IntegrationTest`). При отсутствии локального SQL Server он будет падать, т.к. использует тип `SQL_VARIANT`.

## Рекомендации (коротко)

1. Реализовать нормализацию JDBC->каноника в `JdbcStreamingResultSetExtractor` (или в отдельном слое), чтобы `DATE` становился `LocalDate`, `DATETIME2` — `Instant` (с явной стратегией timezone), `DECIMAL` — `BigDecimal`.
2. Доработать `TypeConverter.convertToJdbc(...)` (или `AvroToRecordTransformer`) для обратной конвертации Avro logical types: `int` date, `long` timestamp, `bytes/ByteBuffer` decimal.
3. Исправить `TypeConverterTest.testFullPipeline_SqlToKafkaToSql_Date` и добавить отсутствующие тест-кейсы для Avro->JDBC.
4. Перенести `JdbcExtractorValidationTest` в `*IntegrationTest` или добавить test-профиль/поднятие SQL Server в CI (иначе `mvn test` нестабилен).
5. Уточнить и привести к единому варианту противоречие в `doc/type-conversion-requirements.md` по поводу включения `__variant_*` в `ColumnMetadata`.

---

### Список ключевых файлов реализации
- Kafka envelope: `src/main/java/ru/pospelov/etl/engine/steps/extractor/kafka/KafkaPartitionExtractor.java`, `src/main/java/ru/pospelov/etl/engine/steps/loader/KafkaByPartitionLoader.java`
- Batch/metadata: `src/main/java/ru/pospelov/etl/engine/model/EtlBatch.java`, `src/main/java/ru/pospelov/etl/engine/model/ColumnMetadata.java`
- sql_variant: `src/main/java/ru/pospelov/etl/engine/model/SqlVariantValue.java`, `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/SqlVariantQueryGenerator.java`, `src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcStreamingResultSetExtractor.java`
- Type conversion: `src/main/java/ru/pospelov/etl/engine/conversion/TypeConverter.java`
- SQL loaders: `src/main/java/ru/pospelov/etl/engine/steps/loader/JdbcLoader.java`, `src/main/java/ru/pospelov/etl/engine/steps/loader/FastSqlServerLoader.java`
