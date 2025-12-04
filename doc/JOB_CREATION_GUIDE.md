# Руководство по созданию и запуску ETL Job

## Введение

ETL Job в EtlEngine состоит из трех компонентов:
- **Extractor** - извлекает данные из источника
- **Transformer** - трансформирует данные
- **Loader** - загружает данные в целевую систему

## Структура EtlJob

```java
EtlJob job = new EtlJob(
    String id,              // Уникальный идентификатор джоба
    String sourceQuery,     // SQL запрос (для SQL extractor) или null
    String target,          // Целевая таблица (для SQL loader) или null
    Map<String, Object> params  // Параметры конфигурации
);
```

## Примеры создания Job'ов

### 1. SQL → Kafka (Экспорт данных)

```java
// Получаем Avro схему из Schema Registry
Schema schema = fetchSchemaFromRegistry("order-events-value");

EtlJob sqlToKafka = new EtlJob(
    "sql-to-kafka",
    "SELECT * FROM SUPPORT.dbo.order_src",  // SQL запрос
    null,                                    // target не нужен для Kafka
    Map.ofEntries(
        Map.entry("extractorType", "sql"),
        Map.entry("loaderType", "kafka"),
        Map.entry("transformerType", "noop"),    // Без трансформации
        Map.entry("topic", "order-events-value"),
        Map.entry("format", "avro"),
        Map.entry("threads", 8),                 // Параллельная обработка
        Map.entry("streamBatchSize", 100_000),   // Размер батча
        Map.entry("avroSchema", schema.toString()),
        Map.entry("keyColumn", "order_id"),      // Колонка для ключа Kafka
        // Партиционирование SQL для параллельной обработки
        Map.entry("partitionColumn", "bucket"),
        Map.entry("partitions", 72)
    )
);

// Запуск
pipelineFactory.create(sqlToKafka).run(sqlToKafka);
```

### 2. Kafka → SQL (Импорт данных)

```java
EtlJob kafkaToSql = new EtlJob(
    "kafka-to-sql",
    null,                               // sourceQuery не нужен для Kafka
    "SUPPORT.dbo.order_dst",           // Целевая таблица
    Map.of(
        "extractorType", "kafka",
        "format", "avro",
        "transformerType", "avro",      // Трансформация из Avro
        "loaderType", "fast-sql",       // Быстрая загрузка через Bulk Copy
        "topic", "order-events-value",
        // Фильтрация по времени
        "startTimestamp", startTime.toEpochMilli(),
        "endTimestamp", endTime.toEpochMilli(),
        "streamBatchSize", 100_000,
        "threads", 8
    )
);

// Запуск
pipelineFactory.create(kafkaToSql).run(kafkaToSql);
```

### 3. SQL → SQL (ETL внутри БД)

```java
EtlJob sqlToSql = new EtlJob(
    "sql-to-sql",
    "SELECT id, name, UPPER(email) as email FROM users WHERE active = 1",
    "dbo.users_processed",
    Map.of(
        "extractorType", "sql",
        "loaderType", "fast-sql",
        "transformerType", "noop",
        "threads", 4,
        "streamBatchSize", 50_000
    )
);
```

## Параметры конфигурации

### Общие параметры

| Параметр | Тип | Описание | Обязательный |
|----------|-----|----------|--------------|
| `extractorType` | String | Тип экстрактора: `sql`, `kafka` | Да |
| `loaderType` | String | Тип лоадера: `kafka`, `sql`, `fast-sql` | Да |
| `transformerType` | String | Тип трансформера: `noop`, `avro` | Да |
| `threads` | Integer | Количество потоков для параллельной обработки | Нет (default: 1) |
| `streamBatchSize` | Integer | Размер батча для потоковой обработки | Нет (default: 10000) |

### SQL Extractor

| Параметр | Описание |
|----------|----------|
| `partitionColumn` | Колонка для партиционирования (например, `bucket`) |
| `partitions` | Количество партиций для параллельной обработки |

**Пример SQL запроса с партиционированием:**
```sql
-- Таблица должна иметь вычисляемую колонку bucket
SELECT * FROM orders WHERE bucket IN (0, 1, 2, ...)
```

### Kafka Extractor

| Параметр | Тип | Описание |
|----------|-----|----------|
| `topic` | String | Имя топика Kafka |
| `format` | String | Формат данных: `avro`, `json` |
| `startTimestamp` | Long | Начальная метка времени (epoch millis) |
| `endTimestamp` | Long | Конечная метка времени (epoch millis) |

### Kafka Loader

| Параметр | Тип | Описание |
|----------|-----|----------|
| `topic` | String | Имя топика Kafka |
| `format` | String | Формат данных: `avro`, `json` |
| `avroSchema` | String | Avro схема (JSON string) |
| `keyColumn` | String | Колонка для использования в качестве ключа сообщения |

### SQL Loader

| Параметр | Описание |
|----------|----------|
| `loaderType: "fast-sql"` | Использует SQL Server Bulk Copy API (намного быстрее) |
| `loaderType: "sql"` | Обычный INSERT (медленнее, но универсальнее) |

## Получение Avro схемы из Schema Registry

```java
private Schema fetchSchemaFromRegistry(String subject) throws Exception {
    String url = "http://localhost:8081/subjects/" + subject + "/versions/latest";
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Accept", "application/vnd.schemaregistry.v1+json")
            .build();

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    String rawSchema = com.jayway.jsonpath.JsonPath.read(response.body(), "$.schema");
    return new Schema.Parser().parse(rawSchema);
}
```

## Запуск Job через Spring Bean

```java
@Service
public class EtlService {

    @Autowired
    private EtlPipelineFactory pipelineFactory;

    public void runExportJob() {
        EtlJob job = new EtlJob(
            "export-orders",
            "SELECT * FROM orders WHERE created_at > ?",
            null,
            Map.of(
                "extractorType", "sql",
                "loaderType", "kafka",
                "transformerType", "noop",
                "topic", "orders-export",
                "threads", 4
            )
        );

        // Синхронный запуск
        pipelineFactory.create(job).run(job);
    }
}
```

## Асинхронный запуск

```java
@Service
public class AsyncEtlService {

    @Autowired
    private EtlPipelineFactory pipelineFactory;

    @Async
    public CompletableFuture<Void> runJobAsync(EtlJob job) {
        return CompletableFuture.runAsync(() -> {
            pipelineFactory.create(job).run(job);
        });
    }
}
```

## Мониторинг выполнения

```java
EtlPipeline pipeline = pipelineFactory.create(job);

// Запуск в отдельном потоке
Thread jobThread = new Thread(() -> pipeline.run(job));
jobThread.start();

// Проверка статуса (если pipeline поддерживает метрики)
while (jobThread.isAlive()) {
    // Логирование прогресса
    Thread.sleep(1000);
}
```

## Обработка ошибок

```java
try {
    pipelineFactory.create(job).run(job);
    System.out.println("Job completed successfully");
} catch (Exception e) {
    System.err.println("Job failed: " + e.getMessage());
    e.printStackTrace();
    // Логирование, уведомления, retry логика и т.д.
}
```

## Best Practices

### 1. Размер батча (streamBatchSize)

```java
// Для больших объемов данных (миллионы строк)
Map.entry("streamBatchSize", 100_000)

// Для средних объемов (тысячи строк)
Map.entry("streamBatchSize", 10_000)

// Для малых объемов или low-latency
Map.entry("streamBatchSize", 1_000)
```

### 2. Количество потоков

```java
// CPU-bound операции
int threads = Runtime.getRuntime().availableProcessors();

// I/O-bound операции (БД, Kafka)
int threads = Runtime.getRuntime().availableProcessors() * 2;

Map.entry("threads", threads)
```

### 3. Партиционирование SQL таблиц

```sql
-- Создание вычисляемой колонки для партиционирования
ALTER TABLE orders
ADD bucket AS (ABS(CHECKSUM(order_id)) % 72) PERSISTED;

-- Создание индекса
CREATE INDEX IX_orders_bucket ON orders(bucket);
```

### 4. Использование fast-sql для больших объемов

```java
// Для загрузки > 10,000 строк используйте fast-sql
Map.entry("loaderType", "fast-sql")  // SQL Server Bulk Copy

// Для малых объемов можно использовать обычный sql
Map.entry("loaderType", "sql")       // Обычный INSERT
```

## Примеры из реального мира

### Экспорт аналитики в Data Lake

```java
EtlJob analyticsExport = new EtlJob(
    "analytics-daily-export",
    """
    SELECT
        order_id,
        customer_id,
        order_date,
        total_amount,
        status
    FROM orders
    WHERE order_date >= DATEADD(day, -1, GETDATE())
    """,
    null,
    Map.of(
        "extractorType", "sql",
        "loaderType", "kafka",
        "transformerType", "noop",
        "topic", "analytics-orders",
        "format", "avro",
        "avroSchema", schema.toString(),
        "threads", 4,
        "streamBatchSize", 50_000
    )
);
```

### Синхронизация между системами

```java
EtlJob sync = new EtlJob(
    "sync-customers",
    "SELECT * FROM source_db.dbo.customers WHERE updated_at > ?",
    "target_db.dbo.customers",
    Map.of(
        "extractorType", "sql",
        "loaderType", "fast-sql",
        "transformerType", "noop",
        "threads", 8,
        "streamBatchSize", 10_000
    )
);
```

## Troubleshooting

### Проблема: OutOfMemoryError

**Решение:** Уменьшите `streamBatchSize` и количество `threads`

```java
Map.entry("streamBatchSize", 10_000)  // Вместо 100_000
Map.entry("threads", 2)                // Вместо 8
```

### Проблема: Медленная загрузка в SQL

**Решение:** Используйте `fast-sql` и увеличьте `streamBatchSize`

```java
Map.entry("loaderType", "fast-sql")
Map.entry("streamBatchSize", 100_000)
```

### Проблема: Kafka lag

**Решение:** Увеличьте количество потоков и партиций топика

```java
Map.entry("threads", 16)  // Больше потоков для чтения
// Также увеличьте количество партиций в Kafka топике
```

## Дополнительные ресурсы

- См. `KafkaToSqlIntegrationTest.java` для полного примера
- См. `EtlPipelineFactory.java` для деталей реализации
- См. `TESTING.md` для запуска тестов
