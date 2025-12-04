# REST API для управления ETL Job'ами

REST API для CRUD операций и ручного запуска ETL job'ов без использования планировщика.

## Конфигурация

API использует in-memory хранилище для job'ов (класс `InMemoryJobRepository`). В будущем может быть заменено на постоянное хранилище в БД (см. пункт 17 в ROADMAP.md).

## Эндпоинты

Базовый путь: `/api/jobs`

### 1. Создать новый job

```
POST /api/jobs
Content-Type: application/json
```

**Тело запроса:**
```json
{
  "id": "sql-to-kafka",
  "sourceQuery": "SELECT * FROM SUPPORT.dbo.order_src",
  "target": null,
  "params": {
    "extractorType": "sql",
    "transformerType": "noop",
    "loaderType": "kafka",
    "topic": "order-events-value",
    "format": "avro",
    "threads": 8,
    "streamBatchSize": 100000,
    "avroSchema": "<schema из registry>",
    "keyColumn": "order_id",
    "partitionColumn": "bucket",
    "partitions": 72
  }
}
```

**Ответ (201 Created):**
```json
{
  "id": "sql-to-kafka",
  "sourceQuery": "SELECT * FROM SUPPORT.dbo.order_src",
  "target": null,
  "params": {
    "extractorType": "sql",
    "transformerType": "noop",
    "loaderType": "kafka",
    "topic": "order-events-value",
    "format": "avro",
    "threads": 8,
    "streamBatchSize": 100000
  }
}
```

**Возможные ошибки:**
- `400 Bad Request` - отсутствуют обязательные параметры
- `409 Conflict` - job с таким id уже существует

### 2. Обновить существующий job

```
PUT /api/jobs/{id}
Content-Type: application/json
```

**Тело запроса:** аналогично POST

**Ответ (200 OK):** обновленный job

**Возможные ошибки:**
- `400 Bad Request` - невалидные данные
- `404 Not Found` - job не найден

### 3. Получить job по id

```
GET /api/jobs/{id}
```

**Ответ (200 OK):**
```json
{
  "id": "sql-to-kafka",
  "sourceQuery": "SELECT * FROM SUPPORT.dbo.order_src",
  "target": null,
  "params": {
    "extractorType": "sql",
    "loaderType": "kafka"
  }
}
```

**Возможные ошибки:**
- `404 Not Found` - job не найден

### 4. Получить все job'ы

```
GET /api/jobs
```

**Ответ (200 OK):**
```json
[
  {
    "id": "sql-to-kafka",
    "sourceQuery": "SELECT * FROM SUPPORT.dbo.order_src",
    "target": null,
    "params": {}
  },
  {
    "id": "kafka-to-sql",
    "sourceQuery": null,
    "target": "SUPPORT.dbo.order_dst",
    "params": {}
  }
]
```

### 5. Удалить job

```
DELETE /api/jobs/{id}
```

**Ответ:** `204 No Content`

**Возможные ошибки:**
- `404 Not Found` - job не найден

### 6. Запустить job вручную

```
POST /api/jobs/{id}/run
Content-Type: application/json (опционально)
```

**Тело запроса (опционально):**
```json
{
  "params": {
    "threads": 16,
    "streamBatchSize": 200000
  }
}
```

Параметры в теле запроса будут объединены с параметрами job'а, переопределяя их при совпадении ключей.

**Ответ (202 Accepted):**
```json
{
  "jobId": "sql-to-kafka",
  "status": "STARTED",
  "message": "Job execution started successfully",
  "startedAt": 1733166000000
}
```

Job выполняется асинхронно в фоновом режиме.

**Возможные ошибки:**
- `404 Not Found` - job не найден
- `500 Internal Server Error` - ошибка при запуске job'а

## Примеры использования

### Пример 1: SQL → Kafka

Создание job'а для экспорта данных из SQL Server в Kafka:

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "id": "orders-sql-to-kafka",
    "sourceQuery": "SELECT * FROM SUPPORT.dbo.order_src",
    "target": null,
    "params": {
      "extractorType": "sql",
      "transformerType": "noop",
      "loaderType": "kafka",
      "topic": "order-events-value",
      "format": "avro",
      "threads": 8,
      "streamBatchSize": 100000,
      "avroSchema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"order_id\",\"type\":\"long\"}]}",
      "keyColumn": "order_id",
      "partitionColumn": "bucket",
      "partitions": 72
    }
  }'
```

Запуск job'а:

```bash
curl -X POST http://localhost:8080/api/jobs/orders-sql-to-kafka/run
```

### Пример 2: Kafka → SQL

Создание job'а для импорта данных из Kafka в SQL Server:

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "id": "orders-kafka-to-sql",
    "sourceQuery": null,
    "target": "SUPPORT.dbo.order_dst",
    "params": {
      "extractorType": "kafka",
      "transformerType": "avro",
      "loaderType": "fast-sql",
      "topic": "order-events-value",
      "format": "avro",
      "startTimestamp": 1733164800000,
      "endTimestamp": 1733168400000,
      "streamBatchSize": 100000,
      "threads": 8
    }
  }'
```

Запуск job'а с переопределением параметров:

```bash
curl -X POST http://localhost:8080/api/jobs/orders-kafka-to-sql/run \
  -H "Content-Type: application/json" \
  -d '{
    "params": {
      "startTimestamp": 1733170000000,
      "endTimestamp": 1733173600000
    }
  }'
```

### Пример 3: Получить список всех job'ов

```bash
curl http://localhost:8080/api/jobs
```

### Пример 4: Удалить job

```bash
curl -X DELETE http://localhost:8080/api/jobs/orders-sql-to-kafka
```

## Обязательные параметры

Каждый job должен содержать следующие обязательные параметры в `params`:

- **extractorType** - тип экстрактора (`sql`, `kafka`)
- **transformerType** - тип трансформера (`noop`, `avro`)
- **loaderType** - тип загрузчика (`jdbc`, `kafka`, `fast-sql`)

### Дополнительные параметры для SQL Extractor:
- **sourceQuery** - SQL-запрос для извлечения данных (в корне объекта, не в params)

### Дополнительные параметры для Kafka Extractor:
- **topic** - имя топика Kafka
- **format** - формат данных (`avro`, `json`)
- **startTimestamp** - начальная временная метка (Unix timestamp в мс)
- **endTimestamp** - конечная временная метка (Unix timestamp в мс)

### Дополнительные параметры для Kafka Loader:
- **topic** - имя топика Kafka
- **format** - формат данных (`avro`, `json`)
- **avroSchema** - схема Avro для сериализации
- **keyColumn** - колонка для ключа Kafka сообщения
- **partitionColumn** - колонка для партиционирования
- **partitions** - количество партиций

### Дополнительные параметры для SQL Loader:
- **target** - целевая таблица (в корне объекта, не в params)

### Общие параметры:
- **threads** - количество потоков для параллельной обработки (по умолчанию 4)
- **streamBatchSize** - размер батча для потоковой обработки (по умолчанию 50000)

## Обработка ошибок

Все ошибки возвращаются в едином формате:

```json
{
  "timestamp": "2025-12-02T18:50:00.000Z",
  "status": 404,
  "error": "Not Found",
  "message": "Job not found with id: non-existent-job"
}
```

HTTP коды состояния:
- `200 OK` - успешное выполнение
- `201 Created` - ресурс создан
- `202 Accepted` - запрос принят (асинхронная операция)
- `204 No Content` - успешное удаление
- `400 Bad Request` - невалидные данные
- `404 Not Found` - ресурс не найден
- `409 Conflict` - конфликт (дубликат)
- `500 Internal Server Error` - внутренняя ошибка сервера

## Архитектура

```
┌─────────────┐
│JobController│
└──────┬──────┘
       │
       ▼
  ┌─────────┐        ┌──────────────┐
  │JobService│───────▶│JobRepository │
  └────┬────┘        └──────────────┘
       │             (In-Memory)
       │
       ▼
┌──────────────────┐
│EtlPipelineFactory│
└──────────────────┘
```

## Следующие шаги

См. пункт 17 в [ROADMAP.md](ROADMAP.md) для перехода на персистентное хранилище job'ов в БД.

## Тестирование

API покрыто unit-тестами с использованием Spring MockMvc и Mockito:

```bash
mvn test -Dtest=JobControllerTest
```

Тесты проверяют:
- ✅ Создание, обновление, удаление и получение job'ов
- ✅ Обработку ошибок (404, 409, 400)
- ✅ Запуск job'ов
- ✅ Валидацию параметров

Все тесты используют `@MockitoBean` вместо устаревшего `@MockBean` для совместимости с Spring Boot 3.4+.

## Связанные документы

- [ROADMAP.md](ROADMAP.md) - План развития ETL Engine
- [SQL_TO_KAFKA.md](SQL_TO_KAFKA.md) - Детали SQL → Kafka
- [KAFKA_TO_SQL.md](KAFKA_TO_SQL.md) - Детали Kafka → SQL
