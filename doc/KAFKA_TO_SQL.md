# Инструкция: Передача данных Kafka → SQL

Данная инструкция описывает процесс настройки и выполнения передачи данных из Apache Kafka топика в SQL Server базу данных.

## Обзор

Передача данных Kafka → SQL включает следующие этапы:
1. **Extract** - извлечение данных из Kafka топика
2. **Transform** - преобразование данных (опционально, из Avro формата)
3. **Load** - загрузка данных в SQL таблицу

## Предварительные требования

1. Запущенный Kafka кластер с топиком, содержащим данные
2. Schema Registry (если используется Avro формат)
3. Запущенный SQL Server с доступом к базе данных
4. Настроенный `application.yml` с параметрами подключения

## Вариант 1: Простая передача (Kafka → SQL, формат String)

### Шаг 1: Подготовка целевой таблицы

Создайте таблицу в SQL Server для приема данных:

```sql
CREATE TABLE target_table (
    key NVARCHAR(255),
    value NVARCHAR(MAX),
    timestamp DATETIME,
    source_partition NVARCHAR(255),
    offset BIGINT
);
```

### Шаг 2: Создание ETL задачи

```java
import ru.pospelov.etl.engine.engine.EtlPipelineFactory;
import ru.pospelov.etl.engine.model.EtlJob;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Instant;
import java.util.Map;

@Autowired
private EtlPipelineFactory pipelineFactory;

// Создание задачи
EtlJob job = new EtlJob(
    "kafka-to-sql-string",           // jobId
    null,                             // sourceQuery - не требуется для Kafka
    "target_table",                   // targetTable - целевая таблица
    Map.of(
        // Типы компонентов
        "extractorType", "kafka",     // Используем KafkaExtractor
        "transformerType", "noop",    // Без преобразований
        "loaderType", "sql",          // Используем JdbcLoader
        
        // Параметры Kafka
        "topic", "my-topic",          // Имя топика
        "startTimestamp", Instant.now().minusSeconds(600).toEpochMilli(),  // Начало периода
        "endTimestamp", Instant.now().toEpochMilli(),                     // Конец периода
        
        // Параметры производительности
        "batchSize", 10000,           // Размер батча
        "threads", 4                  // Количество потоков
    )
);
```

### Шаг 3: Запуск пайплайна

```java
EtlPipeline pipeline = pipelineFactory.create(job);
pipeline.run(job);
```

### Результат

Данные из Kafka топика `my-topic` за последние 10 минут будут загружены в таблицу `target_table`.

## Вариант 2: Передача с Avro схемой (Kafka → SQL, формат Avro)

### Шаг 1: Подготовка целевой таблицы

Создайте таблицу, соответствующую структуре Avro схемы:

```sql
CREATE TABLE orders_table (
    order_id NVARCHAR(64),
    customer_id NVARCHAR(64),
    order_date NVARCHAR(32),
    delivery_date NVARCHAR(32),
    status NVARCHAR(32),
    total_amount FLOAT,
    currency NVARCHAR(8),
    item_count INT,
    shipping_address NVARCHAR(800),
    billing_address NVARCHAR(800),
    -- ... остальные поля согласно Avro схеме
    timestamp DATETIME,
    source_partition NVARCHAR(255),
    offset BIGINT
);
```

### Шаг 2: Создание ETL задачи с Avro

```java
EtlJob job = new EtlJob(
    "kafka-to-sql-avro",
    null,
    "orders_table",
    Map.of(
        "extractorType", "kafka",
        "format", "avro",                      // Указываем Avro формат
        "transformerType", "avro",             // Преобразование из Avro
        "loaderType", "fast-sql",             // Используем быстрый загрузчик
        "topic", "orders-topic",
        "startTimestamp", Instant.now().minusSeconds(3600).toEpochMilli(),  // Последний час
        "endTimestamp", Instant.now().toEpochMilli(),
        "batchSize", 100000,
        "threads", 8
    )
);
```

### Шаг 3: Запуск пайплайна

```java
EtlPipeline pipeline = pipelineFactory.create(job);
pipeline.run(job);
```

### Особенности Avro трансформера

`AvroToRecordTransformer` автоматически:
- Извлекает все поля из Avro GenericRecord
- Преобразует boolean значения в 0/1 для совместимости с SQL Server
- Сохраняет все остальные типы данных как есть

## Вариант 3: Использование FastSqlServerLoader для высокой производительности

Для больших объемов данных рекомендуется использовать `fast-sql` загрузчик:

```java
EtlJob job = new EtlJob(
    "kafka-to-sql-fast",
    null,
    "large_table",
    Map.of(
        "extractorType", "kafka",
        "format", "avro",
        "transformerType", "avro",
        "loaderType", "fast-sql",             // Быстрый загрузчик
        "topic", "bulk-topic",
        "startTimestamp", Instant.now().minusSeconds(86400).toEpochMilli(),  // Последние 24 часа
        "endTimestamp", Instant.now().toEpochMilli(),
        "batchSize", 100000,                  // Большой размер батча
        "threads", 8                          // Много потоков
    )
);
```

### Преимущества FastSqlServerLoader

- Использует SQL Server Bulk Copy API
- Отключает проверки ограничений и триггеры для максимальной скорости
- Использует блокировку таблицы
- Оптимизирован для массовой загрузки

## Параметры конфигурации

### Общие параметры

| Параметр | Тип | Описание | По умолчанию |
|----------|-----|----------|--------------|
| `extractorType` | String | Тип извлекателя: `"kafka"` | Обязательно |
| `transformerType` | String | Тип трансформера: `"noop"`, `"avro"` | Обязательно |
| `loaderType` | String | Тип загрузчика: `"sql"`, `"fast-sql"` | Обязательно |
| `batchSize` | Integer | Размер батча для обработки | 1000 |
| `threads` | Integer | Количество потоков | 4 |

### Параметры для Kafka Extractor

| Параметр | Тип | Описание | По умолчанию |
|----------|-----|----------|--------------|
| `topic` | String | Имя Kafka топика | Обязательно |
| `startTimestamp` | Long | Начальная временная метка (миллисекунды) | Обязательно |
| `endTimestamp` | Long | Конечная временная метка (миллисекунды) | Обязательно |
| `format` | String | Формат данных: `"string"` или `"avro"` | `"string"` |
| `threads` | Integer | Количество потоков для параллельной обработки партиций | 4 |

### Параметры для SQL Loaders

| Параметр | Тип | Описание | По умолчанию |
|----------|-----|----------|--------------|
| `targetTable` | String | Имя целевой таблицы | Обязательно |
| `batchSize` | Integer | Размер батча для вставки | 1000 |
| `threads` | Integer | Количество потоков | 4 |

## Примеры использования

### Пример 1: Простая передача строк

```java
EtlJob job = new EtlJob(
    "simple-kafka-to-sql",
    null,
    "messages_table",
    Map.of(
        "extractorType", "kafka",
        "transformerType", "noop",
        "loaderType", "sql",
        "topic", "messages",
        "startTimestamp", Instant.now().minusSeconds(300).toEpochMilli(),
        "endTimestamp", Instant.now().toEpochMilli()
    )
);
pipelineFactory.create(job).run(job);
```

### Пример 2: Передача Avro данных

```java
EtlJob job = new EtlJob(
    "avro-kafka-to-sql",
    null,
    "orders_table",
    Map.of(
        "extractorType", "kafka",
        "format", "avro",
        "transformerType", "avro",
        "loaderType", "fast-sql",
        "topic", "order-events",
        "startTimestamp", Instant.now().minusSeconds(3600).toEpochMilli(),
        "endTimestamp", Instant.now().toEpochMilli(),
        "batchSize", 50000,
        "threads", 6
    )
);
pipelineFactory.create(job).run(job);
```

### Пример 3: Массовая загрузка за период

```java
// Загрузка данных за последние 7 дней
Instant endTime = Instant.now();
Instant startTime = endTime.minusSeconds(7 * 24 * 60 * 60);

EtlJob job = new EtlJob(
    "bulk-load-week",
    null,
    "historical_data",
    Map.of(
        "extractorType", "kafka",
        "format", "avro",
        "transformerType", "avro",
        "loaderType", "fast-sql",
        "topic", "events-topic",
        "startTimestamp", startTime.toEpochMilli(),
        "endTimestamp", endTime.toEpochMilli(),
        "batchSize", 100000,
        "threads", 8
    )
);
pipelineFactory.create(job).run(job);
```

### Пример 4: Обработка конкретного временного окна

```java
// Загрузка данных за конкретный день
Instant start = Instant.parse("2025-01-15T00:00:00Z");
Instant end = Instant.parse("2025-01-15T23:59:59Z");

EtlJob job = new EtlJob(
    "load-specific-day",
    null,
    "daily_data",
    Map.of(
        "extractorType", "kafka",
        "format", "avro",
        "transformerType", "avro",
        "loaderType", "fast-sql",
        "topic", "events-topic",
        "startTimestamp", start.toEpochMilli(),
        "endTimestamp", end.toEpochMilli(),
        "batchSize", 100000,
        "threads", 8
    )
);
pipelineFactory.create(job).run(job);
```

## Работа с временными метками

### Определение временного окна

Kafka Extractor использует временные метки сообщений для фильтрации данных:

```java
// Последние 10 минут
long start = Instant.now().minusSeconds(600).toEpochMilli();
long end = Instant.now().toEpochMilli();

// Последний час
long start = Instant.now().minusSeconds(3600).toEpochMilli();
long end = Instant.now().toEpochMilli();

// Конкретная дата и время
Instant start = Instant.parse("2025-01-15T10:00:00Z");
Instant end = Instant.parse("2025-01-15T11:00:00Z");
long startMillis = start.toEpochMilli();
long endMillis = end.toEpochMilli();
```

### Важные замечания

- Kafka Extractor обрабатывает все партиции топика параллельно
- Данные фильтруются по временной метке сообщения (`timestamp`)
- Если сообщение не имеет временной метки, оно может быть пропущено

## Структура данных в SQL таблице

### Для строкового формата

Таблица должна содержать поля:
- `key` - ключ Kafka сообщения (если есть)
- `value` - значение сообщения (строка)
- `timestamp` - временная метка записи
- `source_partition` - партиция Kafka
- `offset` - смещение в партиции

### Для Avro формата

Таблица должна содержать все поля из Avro схемы плюс служебные поля:
- Все поля из Avro схемы
- `timestamp` - временная метка записи (опционально)
- `source_partition` - партиция Kafka (опционально)
- `offset` - смещение в партиции (опционально)

## Мониторинг и отладка

### Проверка данных в SQL

```sql
-- Проверка количества загруженных записей
SELECT COUNT(*) FROM target_table;

-- Проверка последних записей
SELECT TOP 100 * FROM target_table ORDER BY timestamp DESC;

-- Проверка данных по партициям
SELECT source_partition, COUNT(*) as count 
FROM target_table 
GROUP BY source_partition;
```

### Логирование

Включите детальное логирование в `application.yml`:

```yaml
logging:
  level:
    ru.pospelov.etl.engine: DEBUG
    org.apache.kafka: INFO
```

### Проверка прогресса

FastSqlServerLoader выводит информацию о прогрессе:

```
INFO: Thread pool-1-thread-1 inserted 100000 rows
INFO: ✅ Fast bulk insert into 'target_table' completed in 12345 ms (1000000 rows, 8 threads)
```

## Оптимизация производительности

1. **Используйте FastSqlServerLoader** для больших объемов данных
2. **Увеличьте batchSize** до 50000-100000 для массовой загрузки
3. **Используйте больше потоков** (8-16) для параллельной обработки
4. **Отключите индексы и триггеры** на целевой таблице перед загрузкой (FastSqlServerLoader делает это автоматически)
5. **Используйте временные окна** для инкрементальной загрузки

## Обработка ошибок

### Типичные проблемы

1. **Таблица не существует**
   - Убедитесь, что целевая таблица создана
   - Проверьте правильность имени таблицы (схема.таблица)

2. **Несоответствие схемы**
   - Для Avro: убедитесь, что структура таблицы соответствует Avro схеме
   - Проверьте типы данных колонок

3. **Проблемы с временными метками**
   - Убедитесь, что в топике есть сообщения в указанном временном диапазоне
   - Проверьте, что сообщения имеют временные метки

4. **Проблемы с подключением**
   - Проверьте доступность Kafka брокеров
   - Проверьте доступность SQL Server
   - Проверьте настройки в `application.yml`

### Рекомендации

- Всегда проверяйте логи приложения при ошибках
- Используйте небольшие временные окна для тестирования
- Проверяйте структуру данных перед массовой загрузкой

## Инкрементальная загрузка

Для регулярной загрузки данных можно использовать инкрементальный подход:

```java
// Сохраняйте последнюю временную метку
Instant lastLoadTime = getLastLoadTime(); // Из БД или конфигурации
Instant currentTime = Instant.now();

EtlJob job = new EtlJob(
    "incremental-load",
    null,
    "target_table",
    Map.of(
        "extractorType", "kafka",
        "format", "avro",
        "transformerType", "avro",
        "loaderType", "fast-sql",
        "topic", "events-topic",
        "startTimestamp", lastLoadTime.toEpochMilli(),
        "endTimestamp", currentTime.toEpochMilli(),
        "batchSize", 100000,
        "threads", 8
    )
);
pipelineFactory.create(job).run(job);

// Сохраните текущее время как последнее время загрузки
saveLastLoadTime(currentTime);
```

## Следующие шаги

После успешной передачи данных Kafka → SQL, вы можете:
- Настроить автоматическую загрузку по расписанию
- Настроить мониторинг и алертинг
- Оптимизировать производительность для вашего объема данных
- Настроить передачу данных SQL → Kafka (см. [SQL_TO_KAFKA.md](SQL_TO_KAFKA.md))

