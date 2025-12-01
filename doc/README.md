# ETL Engine - Документация

## Описание проекта

ETL Engine — это Spring Boot приложение для организации ETL (Extract-Transform-Load) процессов. Движок позволяет конфигурировать и запускать пайплайны передачи данных между различными источниками и приемниками, такими как SQL Server и Apache Kafka.

## Архитектура

Проект построен на модульной архитектуре с использованием паттерна реестра компонентов:

```
┌─────────────────────────────────────────┐
│      EtlJobRunner (runner)              │
└──────────────────┬──────────────────────┘
                   │
                   v
┌─────────────────────────────────────────┐
│      EtlPipelineFactory                 │
└──────────────────┬──────────────────────┘
                   │
                   v
┌─────────────────────────────────────────┐
│      EtlPipeline (engine)               │
└──────────────────┬──────────────────────┘
                   │
    ┌──────────────┼──────────────┐
    │              │              │
    v              v              v
┌─────────┐  ┌──────────┐  ┌─────────┐
│Extractor│  │Transformer│  │ Loader  │
└─────────┘  └──────────┘  └─────────┘
```

### Основные компоненты

#### 1. EtlJob
Модель задачи ETL, содержащая:
- `jobId` - уникальный идентификатор задачи
- `sourceQuery` - SQL запрос для извлечения данных (для SQL источников)
- `targetTable` - целевая таблица для загрузки данных (для SQL приемников)
- `parameters` - карта параметров конфигурации

#### 2. EtlPipeline
Интерфейс пайплайна, выполняющий последовательность операций:
1. **Extract** - извлечение данных из источника
2. **Transform** - преобразование данных
3. **Load** - загрузка данных в приемник

#### 3. EtlComponentRegistry
Реестр компонентов, автоматически регистрирующий все доступные:
- Extractors (извлекатели данных)
- Transformers (трансформеры)
- Loaders (загрузчики)

#### 4. EtlPipelineFactory
Фабрика для создания пайплайнов на основе конфигурации задачи.

## Компоненты ETL

### Extractors (Извлекатели)

#### JdbcExtractor (`extractorType: "sql"`)
Извлекает данные из SQL базы данных.

**Параметры:**
- `sourceQuery` или `query` - SQL запрос для выборки данных
- `streamBatchSize` - размер батча для потоковой обработки (по умолчанию: 50000)
- `threads` - количество потоков для параллельной обработки (по умолчанию: 4)
- `partitions` - количество партиций для партиционированного режима
- `partitionColumn` - колонка для партиционирования
- `avroSchema` - схема Avro в виде строки (опционально)
- `keyColumn` - колонка для использования в качестве ключа Kafka

**Режимы работы:**
- **Offset режим**: использует `OFFSET ... ROWS FETCH NEXT ... ROWS ONLY` для пагинации
- **Partition режим**: разделяет данные по значениям `partitionColumn`

#### KafkaExtractor (`extractorType: "kafka"`)
Извлекает данные из Apache Kafka топика.

**Параметры:**
- `topic` - имя Kafka топика
- `startTimestamp` - начальная временная метка (миллисекунды)
- `endTimestamp` - конечная временная метка (миллисекунды)
- `threads` - количество потоков (по умолчанию: 4)
- `format` - формат данных: `"string"` или `"avro"` (по умолчанию: `"string"`)

### Transformers (Трансформеры)

#### NoopTransformer (`transformerType: "noop"`)
Трансформер без преобразований, возвращает данные как есть.

#### AvroToRecordTransformer (`transformerType: "avro"`)
Преобразует Avro GenericRecord в обычные записи EtlRecord для загрузки в SQL.

**Особенности:**
- Автоматически преобразует boolean значения в 0/1 для совместимости с SQL Server
- Извлекает все поля из Avro схемы

#### RecordToAvroTransformer (`transformerType: "record-to-avro"`)
Преобразует обычные записи в Avro GenericRecord.

**Параметры:**
- `avroSchema` - объект Schema Avro

### Loaders (Загрузчики)

#### JdbcLoader (`loaderType: "sql"`)
Загружает данные в SQL базу данных используя batch INSERT.

**Параметры:**
- `targetTable` - целевая таблица
- `batchSize` - размер батча (по умолчанию: 1000)
- `threads` - количество потоков (по умолчанию: 4)

#### FastSqlServerLoader (`loaderType: "fast-sql"`)
Высокопроизводительный загрузчик для SQL Server, использующий SQLServerBulkCopy.

**Параметры:**
- `targetTable` - целевая таблица
- `batchSize` - размер батча (по умолчанию: 1000)
- `threads` - количество потоков (по умолчанию: 4)

**Особенности:**
- Использует SQL Server Bulk Copy API
- Отключает проверки ограничений и триггеры для максимальной производительности
- Использует блокировку таблицы

#### KafkaLoader (`loaderType: "kafka"`)
Загружает данные в Apache Kafka топик.

**Параметры:**
- `topic` - имя Kafka топика
- `format` - формат данных: `"string"` или `"avro"` (по умолчанию: `"string"`)

**Особенности:**
- Использует ключ из поля `"key"` записи, если он присутствует
- Значение берется из поля `"value"` записи

## Конфигурация

### application.yml

```yaml
spring:
  application:
    name: EtlEngine
  datasource:
    url: jdbc:sqlserver://localhost:1433;databaseName=SUPPORT;encrypt=true;trustServerCertificate=true;
    username: sa
    password: verYs3cret
    driver-class-name: com.microsoft.sqlserver.jdbc.SQLServerDriver
  sql.init:
    mode: always
    platform: sqlserver

kafka:
  bootstrap-servers: localhost:29092,localhost:39092,localhost:49092
  schema-registry-url: http://localhost:8081

logging:
  level:
    root: INFO
    org.apache.kafka: WARN
```

### Docker Compose

Проект включает `compose.yaml` с настройкой:
- SQL Server (порт 1433)
- Kafka кластер (3 брокера, 3 контроллера)
- Schema Registry (порт 8081)
- AKHQ - веб-интерфейс для Kafka (порт 8089)
- KSQLDB Server (порт 50001)

## Использование

### Создание ETL задачи

```java
EtlJob job = new EtlJob(
    "job-id",
    "SELECT * FROM source_table",  // sourceQuery (для SQL extractor)
    "target_table",                 // targetTable (для SQL loader)
    Map.of(
        "extractorType", "sql",
        "transformerType", "noop",
        "loaderType", "kafka",
        "topic", "my-topic",
        "format", "avro",
        "streamBatchSize", 50000,
        "threads", 8
    )
);
```

### Запуск пайплайна

```java
@Autowired
private EtlPipelineFactory pipelineFactory;

EtlPipeline pipeline = pipelineFactory.create(job);
pipeline.run(job);
```

## Примеры использования

См. подробные инструкции:
- [SQL → Kafka](SQL_TO_KAFKA.md) - передача данных из SQL в Kafka
- [Kafka → SQL](KAFKA_TO_SQL.md) - передача данных из Kafka в SQL

## Технологии

- **Java 21**
- **Spring Boot 3.5.0**
- **Apache Kafka 3.8.1**
- **Confluent Schema Registry 7.5.0**
- **SQL Server JDBC Driver 12.6.1**
- **Apache Avro 1.11.3**
- **Lombok 1.18.30**

## Производительность

Движок поддерживает:
- Параллельную обработку данных (многопоточность)
- Батчинг для оптимизации операций ввода/вывода
- Партиционирование для больших объемов данных
- Bulk операции для SQL Server

## Ограничения

- SQL Server Bulk Copy работает только с SQL Server
- Avro схемы должны быть зарегистрированы в Schema Registry
- Для партиционированного режима требуется колонка с числовыми значениями

## Разработка

### Структура проекта

```
src/main/java/ru/pospelov/etl/engine/
├── config/          # Конфигурация (Kafka клиенты)
├── engine/          # Ядро ETL (Pipeline, Factory, Registry)
├── model/           # Модели данных (EtlJob, EtlRecord, EtlBulkRecord)
├── runner/          # Запуск задач (EtlJobRunner)
└── steps/
    ├── extractor/   # Извлекатели данных
    ├── transformer/ # Трансформеры
    └── loader/       # Загрузчики
```

### Добавление нового компонента

1. Реализуйте соответствующий интерфейс (`Extractor`, `Transformer`, или `Loader`)
2. Добавьте аннотацию `@Component`
3. Реализуйте метод `getType()` для регистрации в реестре
4. Компонент будет автоматически зарегистрирован через Spring DI

## Лицензия

© 2025 ru.pospelov.etl.engine

