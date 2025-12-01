# ETL Engine - Оглавление документации

## Основная документация

1. [README.md](README.md) - Общая документация проекта
   - Описание и архитектура
   - Компоненты ETL
   - Конфигурация
   - Использование

## Инструкции по использованию

2. [SQL_TO_KAFKA.md](SQL_TO_KAFKA.md) - Передача данных SQL → Kafka
   - Простая передача (String формат)
   - Передача с Avro схемой
   - Партиционированная передача больших объемов
   - Примеры и оптимизация

3. [KAFKA_TO_SQL.md](KAFKA_TO_SQL.md) - Передача данных Kafka → SQL
   - Простая передача (String формат)
   - Передача с Avro схемой
   - Использование FastSqlServerLoader
   - Инкрементальная загрузка
   - Примеры и оптимизация

4. [METRICS.md](METRICS.md) - Метрики и мониторинг
   - Получение метрик и статусов задач
   - Подписка на события выполнения
   - Структура метрик
   - Примеры использования
   - API Reference

## План развития

5. [ROADMAP.md](ROADMAP.md) - План развития ETL Engine
   - Критичные задачи (высокий приоритет)
   - Важные задачи (средний приоритет)
   - Желательные задачи (низкий приоритет)
   - Детальное описание каждой задачи

## Быстрый старт

### SQL → Kafka

```java
EtlJob job = new EtlJob(
    "sql-to-kafka",
    "SELECT * FROM source_table",
    null,
    Map.of(
        "extractorType", "sql",
        "transformerType", "noop",
        "loaderType", "kafka",
        "topic", "my-topic"
    )
);
pipelineFactory.create(job).run(job);
```

### Kafka → SQL

```java
EtlJob job = new EtlJob(
    "kafka-to-sql",
    null,
    "target_table",
    Map.of(
        "extractorType", "kafka",
        "transformerType", "noop",
        "loaderType", "sql",
        "topic", "my-topic",
        "startTimestamp", Instant.now().minusSeconds(600).toEpochMilli(),
        "endTimestamp", Instant.now().toEpochMilli()
    )
);
pipelineFactory.create(job).run(job);
```

## Компоненты

### Extractors (Извлекатели)
- `sql` - JdbcExtractor - извлечение из SQL базы данных
- `kafka` - KafkaExtractor - извлечение из Kafka топика

### Transformers (Трансформеры)
- `noop` - NoopTransformer - без преобразований
- `avro` - AvroToRecordTransformer - преобразование Avro → Record
- `record-to-avro` - RecordToAvroTransformer - преобразование Record → Avro

### Loaders (Загрузчики)
- `sql` - JdbcLoader - загрузка в SQL через batch INSERT
- `fast-sql` - FastSqlServerLoader - быстрая загрузка в SQL Server через Bulk Copy
- `kafka` - KafkaLoader - загрузка в Kafka топик

## Полезные ссылки

- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [SQL Server Bulk Copy](https://learn.microsoft.com/en-us/sql/connect/jdbc/using-bulk-copy-with-the-jdbc-driver)

