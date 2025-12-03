# ETL Engine

ETL (Extract-Transform-Load) движок на базе Spring Boot, позволяющий конфигурировать и запускать пайплайны по расписанию или вручную. Все компоненты ETL описываются в виде интерфейсов и подключаются по типу через реестр.

## 🧩 Архитектура

```
                             +--------------------------+
                             |   EtlJobRunner (runner)  |
                             +------------+-------------+
                                          |
                                          v
                           +--------------+---------------+
                           |     EtlPipelineFactory       |
                           +--------------+---------------+
                                          |
                                          v
                    +---------------------+---------------------+
                    |           EtlPipeline (engine)           |
                    +---------------------+---------------------+
                                          |
         +------------------------+-------+-------+-----------------------+
         |                        |               |                       |
         v                        v               v                       v
+------------------+   +------------------+   +----------------+   +--------------------+
| Extractor (kafka)|   | Transformer      |   | Loader (sql)   |   | EtlComponentRegistry|
| extractor/*      |   | transformer/*    |   | loader/*       |   +--------------------+
+------------------+   +------------------+   +----------------+
        ^                       ^                    ^
        |                       |                    |
        |                       |                    |
  [Interface]            [Interface]           [Interface]
```
## 🚀 Как работает

1. Пользователь задаёт EtlJob с параметрами (типы шагов, параметры запроса и т.п.)
2. `EtlPipelineFactory` создаёт `EtlPipeline` из нужных компонентов
3. `EtlPipeline.run()` вызывает: `extract → transform → load`
4. Все компоненты выбираются через `EtlComponentRegistry`

## 🛠 Зависимости
- Java 21
- Spring Boot 3.2+
- Lombok
- JDBC (MS SQL Server / PostgreSQL / etc.)

## 📖 Документация

### Для пользователей
- **[Быстрый старт](doc/QUICK_START_EXAMPLE.md)** - Запуск первого ETL Job за 5 минут
- **[Руководство по созданию Job](doc/JOB_CREATION_GUIDE.md)** - Полное руководство с примерами
- **[API документация](doc/API.md)** - REST API для управления джобами

### Для разработчиков
- **[Тестирование](TESTING.md)** - Запуск unit, integration и E2E тестов
- **[Roadmap](doc/ROADMAP.md)** - План развития проекта

## 🧪 Тесты

```bash
# Быстрые тесты (unit + integration)
mvn test  # ~8 секунд, 70 тестов

# E2E тест (требует Kafka + SQL Server)
mvn test -Dtest=KafkaToSqlIntegrationTest  # ~40 секунд
```

См. [TESTING.md](TESTING.md) для подробностей.

## 🚀 Быстрый запуск

### Предварительные требования
- Java 21
- SQL Server (localhost:1433)
- Kafka кластер (localhost:29092)
- Schema Registry (localhost:8081)

### Запуск приложения

```bash
# Сборка
mvn clean package

# Запуск
java -jar target/EtlEngine-0.0.1-SNAPSHOT.jar
```

### Пример: Экспорт данных SQL → Kafka

```java
EtlJob job = new EtlJob(
    "export-orders",
    "SELECT * FROM orders",
    null,
    Map.of(
        "extractorType", "sql",
        "loaderType", "kafka",
        "transformerType", "noop",
        "topic", "orders-export",
        "format", "avro",
        "threads", 8
    )
);

pipelineFactory.create(job).run(job);
```

См. [QUICK_START_EXAMPLE.md](doc/QUICK_START_EXAMPLE.md) для полных примеров.

## ✨ Возможности

### Источники данных (Extractors)
- ✅ **SQL** - Любые JDBC-совместимые БД (SQL Server, PostgreSQL и т.д.)
- ✅ **Kafka** - Чтение из Kafka топиков (Avro, JSON)

### Трансформеры
- ✅ **NoOp** - Без трансформации
- ✅ **Avro** - Преобразование Avro → SQL

### Загрузчики (Loaders)
- ✅ **SQL** - Обычный INSERT
- ✅ **Fast SQL** - SQL Server Bulk Copy API (в 10-50x быстрее)
- ✅ **Kafka** - Запись в Kafka (Avro, JSON)

### Оптимизация
- ✅ **Потоковая обработка** - Обработка батчами, экономия памяти
- ✅ **Параллелизм** - Многопоточная обработка
- ✅ **Партиционирование** - SQL партиции для параллельного чтения
- ✅ **Bulk операции** - SQL Server Bulk Copy для быстрой загрузки

### API и интеграция
- ✅ **REST API** - Управление джобами через HTTP
- ✅ **JPA** - Хранение джобов в БД
- ✅ **Spring Boot** - Полная интеграция с Spring экосистемой

## 📊 Производительность

Из E2E теста (`KafkaToSqlIntegrationTest`):
- **1,000,000 записей SQL → Kafka**: ~9 секунд (111,111 записей/сек)
- **1,000,000 записей Kafka → SQL**: ~10 секунд (100,000 записей/сек)
- **Полный цикл SQL → Kafka → SQL**: ~40 секунд

## 📅 Roadmap

См. [doc/ROADMAP.md](doc/ROADMAP.md) для подробного плана развития.

---

© 2025 ru.pospelov.etl.engine
