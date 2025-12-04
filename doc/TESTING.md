# Запуск тестов

## Быстрые тесты (Unit + Integration)

```bash
mvn test
```

- ✅ Запускает 70 тестов (unit + integration)
- ✅ Время выполнения: ~8 секунд
- ✅ Не требует внешних зависимостей
- ✅ **Используется в CI/CD**

## E2E тесты

### Предварительные требования:
- SQL Server запущен (localhost:1433)
- Kafka кластер запущен (localhost:29092, 39092, 49092)
- Schema Registry запущен (localhost:8081)

### Запуск E2E теста:

```bash
mvn test -Dtest=KafkaToSqlIntegrationTest
```

- Проверяет полный цикл: SQL → Kafka → SQL
- Обрабатывает 1,000,000 записей
- Время выполнения: ~40-50 секунд

## CI/CD

### GitHub Actions пример:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up JDK 21
        uses: actions/setup-java@v3
        with:
          java-version: '21'
      - name: Run tests
        run: mvn test  # E2E тесты исключены

  e2e-tests:
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'  # Только на main
    services:
      sqlserver:
        image: mcr.microsoft.com/mssql/server:2019-latest
      kafka:
        image: confluentinc/cp-kafka:7.5.0
    steps:
      - uses: actions/checkout@v3
      - name: Set up JDK 21
        uses: actions/setup-java@v3
        with:
          java-version: '21'
      - name: Run E2E tests
        run: mvn test -Dtest=KafkaToSqlIntegrationTest
```

## Как это работает

**По умолчанию E2E тесты исключены:**
- E2E тесты имеют суффикс `*IntegrationTest.java`
- Maven Surefire настроен исключать эти тесты: `<exclude>**/*IntegrationTest.java</exclude>`
- Это ускоряет обычный запуск тестов (8 сек вместо 50 сек)
- Не требует внешних зависимостей (Kafka, SQL Server)

**Для запуска E2E:**
- Явно указываем класс теста: `-Dtest=KafkaToSqlIntegrationTest`
- Это обходит исключение по паттерну имени файла

## Архитектура тестов

```
src/test/java/
├── ru/pospelov/etl/engine/
│   ├── KafkaToSqlIntegrationTest.java  (@Tag("e2e") - End-to-End)
│   ├── EtlPipelineMetricsTest.java      (Integration)
│   ├── EtlErrorHandlingTest.java        (Integration)
│   └── JobValidationTest.java           (Unit)
```

### Типы тестов:

1. **Unit тесты** - тестируют отдельные компоненты с моками
2. **Integration тесты** - тестируют взаимодействие компонентов
3. **E2E тесты** (`@Tag("e2e")`) - тестируют всю систему с реальными зависимостями

## Отладка

### Запуск конкретного теста:
```bash
mvn test -Dtest=JobValidationTest
```

### Пропустить все тесты:
```bash
mvn install -DskipTests
```
