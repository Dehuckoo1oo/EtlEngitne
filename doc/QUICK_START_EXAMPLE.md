# Быстрый старт: Запуск первого ETL Job

## Предварительные требования

1. SQL Server запущен на `localhost:1433`
2. Kafka запущен на `localhost:29092`
3. Schema Registry запущен на `localhost:8081`

## Шаг 1: Подготовка тестовых данных

### Создание таблицы источника

```sql
-- Подключитесь к SQL Server
USE SUPPORT;

-- Создайте тестовую таблицу
CREATE TABLE dbo.test_orders (
    order_id INT PRIMARY KEY,
    customer_name NVARCHAR(100),
    order_date DATETIME,
    total_amount DECIMAL(10,2)
);

-- Добавьте тестовые данные
INSERT INTO dbo.test_orders VALUES
(1, 'John Doe', '2025-12-01', 100.50),
(2, 'Jane Smith', '2025-12-02', 250.00),
(3, 'Bob Johnson', '2025-12-03', 75.25);
```

## Шаг 2: Создание Avro схемы в Schema Registry

```bash
curl -X POST http://localhost:8081/subjects/test-orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"order_id\",\"type\":\"int\"},{\"name\":\"customer_name\",\"type\":\"string\"},{\"name\":\"order_date\",\"type\":\"string\"},{\"name\":\"total_amount\",\"type\":\"double\"}]}"
  }'
```

## Шаг 3: Создание простого Java приложения

### SimpleEtlExample.java

```java
package ru.pospelov.etl.engine.examples;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import ru.pospelov.etl.engine.engine.EtlPipelineFactory;
import ru.pospelov.etl.engine.model.EtlJob;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

@SpringBootApplication
public class SimpleEtlExample {

    @Autowired
    private EtlPipelineFactory pipelineFactory;

    public static void main(String[] args) {
        SpringApplication.run(SimpleEtlExample.class, args);
    }

    @Bean
    public CommandLineRunner run() {
        return args -> {
            System.out.println("=== Запуск ETL Job: SQL → Kafka ===");

            // 1. Получаем схему из Schema Registry
            Schema schema = fetchSchema("test-orders-value");
            System.out.println("✓ Схема получена из Schema Registry");

            // 2. Создаем ETL Job
            EtlJob job = new EtlJob(
                "example-sql-to-kafka",
                "SELECT order_id, customer_name, order_date, total_amount FROM dbo.test_orders",
                null,
                Map.of(
                    "extractorType", "sql",
                    "loaderType", "kafka",
                    "transformerType", "noop",
                    "topic", "test-orders",
                    "format", "avro",
                    "avroSchema", schema.toString(),
                    "keyColumn", "order_id",
                    "threads", 1
                )
            );
            System.out.println("✓ ETL Job создан");

            // 3. Запускаем Job
            System.out.println("→ Запуск обработки...");
            pipelineFactory.create(job).run(job);

            System.out.println("✓ Job завершен успешно!");
            System.out.println("✓ Данные отправлены в Kafka топик: test-orders");
        };
    }

    private Schema fetchSchema(String subject) throws Exception {
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
}
```

## Шаг 4: Запуск приложения

```bash
# Соберите проект
mvn clean package

# Запустите приложение
java -jar target/EtlEngine-0.0.1-SNAPSHOT.jar
```

## Шаг 5: Проверка результата

### Проверка данных в Kafka

```bash
# Подключитесь к Kafka контейнеру
docker exec -it kafka bash

# Прочитайте сообщения из топика
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-orders \
  --from-beginning
```

## Пример 2: Kafka → SQL (обратная загрузка)

```java
@Bean
public CommandLineRunner runKafkaToSql() {
    return args -> {
        System.out.println("=== Запуск ETL Job: Kafka → SQL ===");

        // Создаем целевую таблицу
        jdbcTemplate.execute("""
            CREATE TABLE dbo.test_orders_copy (
                order_id INT PRIMARY KEY,
                customer_name NVARCHAR(100),
                order_date NVARCHAR(50),
                total_amount FLOAT
            )
        """);

        // Создаем Job для загрузки из Kafka
        EtlJob job = new EtlJob(
            "example-kafka-to-sql",
            null,
            "dbo.test_orders_copy",
            Map.of(
                "extractorType", "kafka",
                "loaderType", "fast-sql",
                "transformerType", "avro",
                "topic", "test-orders",
                "format", "avro",
                "threads", 1
            )
        );

        pipelineFactory.create(job).run(job);
        System.out.println("✓ Данные загружены из Kafka в SQL");

        // Проверяем результат
        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM dbo.test_orders_copy",
            Integer.class
        );
        System.out.println("✓ Загружено записей: " + count);
    };
}
```

## Пример 3: Большой объем данных (1M записей)

```java
@Bean
public CommandLineRunner runBigData() {
    return args -> {
        // 1. Генерируем миллион записей
        System.out.println("Генерация 1,000,000 тестовых записей...");
        jdbcTemplate.execute("""
            DECLARE @i INT = 1;
            WHILE @i <= 1000000
            BEGIN
                INSERT INTO dbo.big_orders (order_id, customer_name, order_date, total_amount)
                VALUES (@i, 'Customer' + CAST(@i AS VARCHAR), GETDATE(), @i * 10.5);
                SET @i = @i + 1;
            END
        """);

        // 2. Запускаем ETL с оптимизацией
        Schema schema = fetchSchema("big-orders-value");

        EtlJob job = new EtlJob(
            "big-data-etl",
            "SELECT * FROM dbo.big_orders",
            null,
            Map.of(
                "extractorType", "sql",
                "loaderType", "kafka",
                "transformerType", "noop",
                "topic", "big-orders",
                "format", "avro",
                "avroSchema", schema.toString(),
                "keyColumn", "order_id",
                "threads", 8,              // Параллельная обработка
                "streamBatchSize", 100_000  // Большой размер батча
            )
        );

        long start = System.currentTimeMillis();
        pipelineFactory.create(job).run(job);
        long duration = System.currentTimeMillis() - start;

        System.out.println("✓ Обработано 1,000,000 записей за " +
                          (duration / 1000) + " секунд");
        System.out.println("✓ Скорость: " +
                          (1_000_000 / (duration / 1000)) + " записей/сек");
    };
}
```

## Типичные ошибки и решения

### Ошибка: "Connection refused" к SQL Server

```bash
# Проверьте, запущен ли SQL Server
docker ps | grep sqlserver

# Проверьте строку подключения в application.yml
spring.datasource.url=jdbc:sqlserver://localhost:1433;databaseName=SUPPORT
```

### Ошибка: "Topic does not exist" в Kafka

```bash
# Создайте топик вручную
kafka-topics --create \
  --bootstrap-server localhost:29092 \
  --topic test-orders \
  --partitions 3 \
  --replication-factor 1
```

### Ошибка: "Schema not found" в Schema Registry

```bash
# Проверьте, зарегистрирована ли схема
curl http://localhost:8081/subjects/test-orders-value/versions
```

## Следующие шаги

1. Прочитайте полное руководство: `doc/JOB_CREATION_GUIDE.md`
2. Изучите интеграционный тест: `KafkaToSqlIntegrationTest.java`
3. Настройте мониторинг и логирование
4. Добавьте обработку ошибок и retry логику
5. Создайте REST API для управления джобами
