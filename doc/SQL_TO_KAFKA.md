# Инструкция: Передача данных SQL → Kafka

Данная инструкция описывает процесс настройки и выполнения передачи данных из SQL Server базы данных в Apache Kafka топик.

## Обзор

Передача данных SQL → Kafka включает следующие этапы:
1. **Extract** - извлечение данных из SQL таблицы
2. **Transform** - преобразование данных (опционально, в Avro формат)
3. **Load** - отправка данных в Kafka топик

## Предварительные требования

1. Запущенный SQL Server с доступом к базе данных
2. Запущенный Kafka кластер
3. Schema Registry (если используется Avro формат)
4. Настроенный `application.yml` с параметрами подключения

## Вариант 1: Простая передача (SQL → Kafka, формат String)

### Шаг 1: Подготовка данных

Убедитесь, что в SQL Server есть таблица с данными:

```sql
CREATE TABLE source_table (
    id INT PRIMARY KEY,
    name NVARCHAR(100),
    email NVARCHAR(100),
    created_date DATETIME
);

INSERT INTO source_table VALUES 
(1, 'Иван Иванов', 'ivan@example.com', GETDATE()),
(2, 'Петр Петров', 'petr@example.com', GETDATE());
```

### Шаг 2: Создание ETL задачи

```java
import ru.pospelov.etl.engine.engine.EtlPipelineFactory;
import ru.pospelov.etl.engine.model.EtlJob;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.Map;

@Autowired
private EtlPipelineFactory pipelineFactory;

// Создание задачи
EtlJob job = new EtlJob(
    "sql-to-kafka-string",           // jobId
    "SELECT * FROM source_table",    // sourceQuery - SQL запрос
    null,                             // targetTable - не требуется для Kafka
    Map.of(
        // Типы компонентов
        "extractorType", "sql",       // Используем JdbcExtractor
        "transformerType", "noop",    // Без преобразований
        "loaderType", "kafka",        // Используем KafkaLoader
        
        // Параметры Kafka
        "topic", "my-topic",          // Имя топика
        
        // Параметры производительности
        "streamBatchSize", 10000,           // Размер батча
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

Данные из таблицы `source_table` будут отправлены в Kafka топик `my-topic` в формате строк.

## Вариант 2: Передача с Avro схемой (SQL → Kafka, формат Avro)

### Шаг 1: Регистрация Avro схемы в Schema Registry

Сначала необходимо зарегистрировать Avro схему в Schema Registry. Пример схемы:

```json
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "ru.pospelov.etl",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "created_date", "type": "string"}
  ]
}
```

Зарегистрируйте схему через REST API Schema Registry:

```bash
curl -X POST http://localhost:8081/subjects/my-topic-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"OrderEvent\",\"namespace\":\"ru.pospelov.etl\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"email\",\"type\":\"string\"},{\"name\":\"created_date\",\"type\":\"string\"}]}"
  }'
```

Или получите схему из файла:

```java
import org.apache.avro.Schema;
import java.nio.file.Files;
import java.nio.file.Paths;

String schemaString = new String(Files.readAllBytes(Paths.get("src/main/resources/avro/order-events-value.avsc")));
Schema schema = new Schema.Parser().parse(schemaString);
```

### Шаг 2: Создание ETL задачи с Avro

```java
import org.apache.avro.Schema;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

// Получение схемы из Schema Registry
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

// Создание задачи
Schema schema = fetchSchemaFromRegistry("my-topic-value");

EtlJob job = new EtlJob(
    "sql-to-kafka-avro",
    "SELECT * FROM source_table",
    null,
    Map.of(
        "extractorType", "sql",
        "transformerType", "record-to-avro",  // Преобразование в Avro
        "loaderType", "kafka",
        "format", "avro",                      // Указываем Avro формат
        "topic", "my-topic",
        "avroSchema", schema,                  // Передаем схему
        "keyColumn", "id",                     // Используем id как ключ Kafka
        "streamBatchSize", 10000,
        "threads", 4
    )
);
```

### Шаг 3: Запуск пайплайна

```java
EtlPipeline pipeline = pipelineFactory.create(job);
pipeline.run(job);
```

## Вариант 3: Партиционированная передача больших объемов данных

Для больших таблиц рекомендуется использовать партиционированный режим:

### Шаг 1: Подготовка таблицы с партиционированием

```sql
-- Создание партиционной функции
CREATE PARTITION FUNCTION pf_bucket(int) 
AS RANGE LEFT FOR VALUES (0,1,2,3,4,5,6,7,8,9);

-- Создание партиционной схемы
CREATE PARTITION SCHEME ps_bucket 
AS PARTITION pf_bucket ALL TO ([PRIMARY]);

-- Создание таблицы с вычисляемой колонкой для партиционирования
CREATE TABLE large_table (
    id INT,
    data NVARCHAR(MAX),
    bucket AS (ABS(CHECKSUM(id)) % 10) PERSISTED
) ON ps_bucket(bucket);
```

### Шаг 2: Создание задачи с партиционированием

```java
Schema schema = fetchSchemaFromRegistry("my-topic-value");

EtlJob job = new EtlJob(
    "sql-to-kafka-partitioned",
    "SELECT * FROM large_table",
    null,
    Map.of(
        "extractorType", "sql",
        "transformerType", "record-to-avro",
        "loaderType", "kafka",
        "format", "avro",
        "topic", "my-topic",
        "avroSchema", schema.toString(),      // Схема как строка
        "keyColumn", "id",
        "partitionColumn", "bucket",          // Колонка для партиционирования
        "partitions", 10,                     // Количество партиций
        "streamBatchSize", 100000,
        "threads", 8
    )
);
```

## Параметры конфигурации

### Общие параметры

| Параметр | Тип | Описание | По умолчанию |
|----------|-----|----------|--------------|
| `extractorType` | String | Тип извлекателя: `"sql"` | Обязательно |
| `transformerType` | String | Тип трансформера: `"noop"`, `"record-to-avro"` | Обязательно |
| `loaderType` | String | Тип загрузчика: `"kafka"` | Обязательно |
| `streamBatchSize` | Integer | Размер батча для обработки | 1000 |
| `threads` | Integer | Количество потоков | 4 |

### Параметры для SQL Extractor

| Параметр | Тип | Описание | По умолчанию |
|----------|-----|----------|--------------|
| `sourceQuery` или `query` | String | SQL запрос для выборки данных | Обязательно |
| `partitionColumn` | String | Колонка для партиционирования | - |
| `partitions` | Integer | Количество партиций | 1 |
| `keyColumn` | String | Колонка для использования как ключ Kafka | - |
| `avroSchema` | String/Schema | Avro схема (строка или объект) | - |

### Параметры для Kafka Loader

| Параметр | Тип | Описание | По умолчанию |
|----------|-----|----------|--------------|
| `topic` | String | Имя Kafka топика | Обязательно |
| `format` | String | Формат данных: `"string"` или `"avro"` | `"string"` |

## Примеры использования

### Пример 1: Простая передача строк

```java
EtlJob job = new EtlJob(
    "simple-transfer",
    "SELECT id, name FROM users WHERE created_date > '2025-01-01'",
    null,
    Map.of(
        "extractorType", "sql",
        "transformerType", "noop",
        "loaderType", "kafka",
        "topic", "users-topic"
    )
);
pipelineFactory.create(job).run(job);
```

### Пример 2: Передача с ключом

```java
EtlJob job = new EtlJob(
    "transfer-with-key",
    "SELECT * FROM orders",
    null,
    Map.of(
        "extractorType", "sql",
        "transformerType", "noop",
        "loaderType", "kafka",
        "topic", "orders-topic",
        "keyColumn", "order_id"  // order_id будет использован как ключ Kafka
    )
);
pipelineFactory.create(job).run(job);
```

### Пример 3: Массовая передача с Avro

```java
Schema schema = fetchSchemaFromRegistry("orders-value");

EtlJob job = new EtlJob(
    "bulk-avro-transfer",
    "SELECT * FROM orders WHERE status = 'PENDING'",
    null,
    Map.of(
        "extractorType", "sql",
        "transformerType", "record-to-avro",
        "loaderType", "kafka",
        "format", "avro",
        "topic", "orders-avro",
        "avroSchema", schema.toString(),
        "keyColumn", "order_id",
        "streamBatchSize", 100000,
        "threads", 8
    )
);
pipelineFactory.create(job).run(job);
```

## Мониторинг и отладка

### Проверка данных в Kafka

Используйте AKHQ (веб-интерфейс на порту 8089) или Kafka консольный consumer:

```bash
# Для строкового формата
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic my-topic --from-beginning

# Для Avro формата (требуется kafka-avro-console-consumer)
kafka-avro-console-consumer --bootstrap-server localhost:29092 \
  --topic my-topic --from-beginning \
  --property schema.registry.url=http://localhost:8081
```

### Логирование

Включите детальное логирование в `application.yml`:

```yaml
logging:
  level:
    ru.pospelov.etl.engine: DEBUG
    org.springframework.jdbc: DEBUG
```

## Оптимизация производительности

1. **Увеличьте streamBatchSize** для больших объемов данных (10000-100000)
2. **Используйте больше потоков** (`threads: 8-16`) для параллельной обработки
3. **Применяйте партиционирование** для очень больших таблиц
4. **Используйте индексы** в SQL запросах для быстрой выборки
5. **Настройте Kafka producer** параметры через `KafkaClientFactory` при необходимости

## Обработка ошибок

При возникновении ошибок проверьте:
1. Доступность SQL Server и корректность запроса
2. Доступность Kafka брокеров
3. Существование топика в Kafka
4. Корректность Avro схемы (если используется)
5. Логи приложения для детальной информации об ошибках

## Следующие шаги

После успешной передачи данных SQL → Kafka, вы можете:
- Настроить передачу данных Kafka → SQL (см. [KAFKA_TO_SQL.md](KAFKA_TO_SQL.md))
- Настроить обработку данных в реальном времени через KSQLDB
- Настроить мониторинг и алертинг

