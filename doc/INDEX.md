# 📚 Документация ETL Engine

## Навигация

### 🚀 Начало работы
1. **[README.md](../README.md)** - Общее описание проекта и возможностей
2. **[QUICK_START_EXAMPLE.md](QUICK_START_EXAMPLE.md)** - Быстрый старт: запуск первого Job за 5 минут
3. **[JOB_CREATION_GUIDE.md](JOB_CREATION_GUIDE.md)** - Подробное руководство по созданию ETL Job

### 🧪 Тестирование
4. **[TESTING.md](../TESTING.md)** - Запуск и настройка тестов (unit, integration, E2E)

### 🔌 API
5. **[API.md](API.md)** - REST API для управления джобами

### 📅 Планирование
6. **[ROADMAP.md](ROADMAP.md)** - План развития проекта

---

## Краткое содержание

### QUICK_START_EXAMPLE.md
Практические примеры для быстрого старта:
- Создание тестовых данных в SQL Server
- Регистрация Avro схемы в Schema Registry
- Простой пример SQL → Kafka
- Обратная загрузка Kafka → SQL
- Обработка больших объемов данных (1M записей)
- Troubleshooting типичных ошибок

**Время чтения:** 10 минут
**Рекомендуется для:** Новых пользователей

### JOB_CREATION_GUIDE.md
Полное руководство по созданию ETL Job:
- Структура EtlJob и параметры
- Все типы Extractor, Transformer, Loader
- Примеры для разных сценариев
- Конфигурация производительности
- Best practices
- Troubleshooting

**Время чтения:** 30 минут
**Рекомендуется для:** Разработчиков, использующих ETL Engine

### TESTING.md
Руководство по тестированию:
- Запуск unit тестов (70 тестов, ~8 сек)
- Запуск E2E тестов (требует Kafka + SQL Server)
- CI/CD конфигурация (GitHub Actions примеры)
- Архитектура тестов
- Как это работает (исключение E2E по паттерну)

**Время чтения:** 10 минут
**Рекомендуется для:** Разработчиков и DevOps

### API.md
REST API документация:
- Создание и управление джобами
- Endpoints для CRUD операций
- Примеры запросов curl
- Схемы данных

**Время чтения:** 15 минут
**Рекомендуется для:** Разработчиков интеграций

### ROADMAP.md
План развития проекта:
- Текущие возможности
- Запланированные функции
- Приоритеты разработки

**Время чтения:** 5 минут
**Рекомендуется для:** Всех пользователей

---

## Быстрая справка

### Создание Job

```java
EtlJob job = new EtlJob(
    "job-id",
    "SELECT * FROM source_table",
    "target_table",
    Map.of(
        "extractorType", "sql",
        "loaderType", "kafka",
        "transformerType", "noop",
        "threads", 8
    )
);

pipelineFactory.create(job).run(job);
```

### Запуск тестов

```bash
# Быстрые тесты
mvn test

# E2E тест
mvn test -Dtest=KafkaToSqlIntegrationTest
```

### Основные параметры

| Параметр | Значения | Описание |
|----------|----------|----------|
| `extractorType` | `sql`, `kafka` | Тип источника данных |
| `loaderType` | `sql`, `fast-sql`, `kafka` | Тип загрузчика |
| `transformerType` | `noop`, `avro` | Тип трансформера |
| `threads` | 1-16 | Количество потоков |
| `streamBatchSize` | 1000-100000 | Размер батча |

---

## Примеры из документации

### SQL → Kafka
См. [JOB_CREATION_GUIDE.md](JOB_CREATION_GUIDE.md#1-sql--kafka-экспорт-данных)

### Kafka → SQL
См. [JOB_CREATION_GUIDE.md](JOB_CREATION_GUIDE.md#2-kafka--sql-импорт-данных)

### Большие объемы
См. [QUICK_START_EXAMPLE.md](QUICK_START_EXAMPLE.md#пример-3-большой-объем-данных-1m-записей)

---

## Полезные ссылки

- **Исходный код**: Интеграционный тест `src/test/java/.../KafkaToSqlIntegrationTest.java`
- **Примеры**: `doc/QUICK_START_EXAMPLE.md`

---

**Последнее обновление:** 2025-12-03
