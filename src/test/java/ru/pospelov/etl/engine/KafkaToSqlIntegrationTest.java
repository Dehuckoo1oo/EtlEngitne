package ru.pospelov.etl.engine;

import lombok.SneakyThrows;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.pospelov.etl.engine.engine.EtlPipelineFactory;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.config.KafkaClientFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

@SpringBootTest
public class KafkaToSqlIntegrationTest {

    @Autowired
    private EtlPipelineFactory pipelineFactory;

    @Autowired
    private KafkaClientFactory kafkaClientFactory;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void kafkaToSql_shouldExtractAndInsert() {
        // Очистим таблицу перед тестом
        jdbcTemplate.execute("DELETE FROM target_table");

        // ==== 1. Отправим сообщение в Kafka ====
        try (Producer<String, String> producer = kafkaClientFactory.createProducer(false)) {
            producer.send(new ProducerRecord<>(
                    "etl_test_topic",
                    "user-123",      // ключ
                    "hello world"    // значение
            )).get(); // дожидаемся отправки
        } catch (Exception e) {
            fail("❌ Не удалось отправить сообщение в Kafka", e);
        }

        EtlJob job = new EtlJob(
                "kafka-to-sql",
                null,
                "target_table",
                Map.of(
                        "extractorType", "kafka",
                        "transformerType", "noop",
                        "loaderType", "fast-sql",
                        "topic", "etl_test_topic",
                        "startTimestamp", Instant.now().minusSeconds(600).toEpochMilli(),
                        "endTimestamp", Instant.now().toEpochMilli(),
                        "batchSize", 100_000,
                        "threads", 6
                )
        );

        pipelineFactory.create(job).run(job);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM target_table");
        assertThat(results).isNotEmpty();
        System.out.println("✅ Записей в таблице: " + results.size());
    }

    @Test
    @SneakyThrows
    void kafkaAvroToSql_shouldExtractAndInsert() {
        // Очистим таблицу
        jdbcTemplate.execute("DELETE FROM SUPPORT.dbo.order_table");

        // Отправим Avro-запись в Kafka
        GenericRecord record = new GenericData.Record(fetchSchemaFromRegistry("order-events-value"));
        record.put("order_id", "ORD-TEST-001");
        record.put("customer_id", "CUST-001");
        record.put("order_date", "2025-06-09");
        record.put("delivery_date", "2025-06-10");
        record.put("status", "PAID");
        record.put("total_amount", 250.0);
        record.put("currency", "USD");
        record.put("item_count", 3);
        record.put("shipping_address", "123 Avro Lane");
        record.put("billing_address", "456 Json Blvd");
        record.put("shipping_zip", "90210");
        record.put("billing_zip", "10001");
        record.put("shipping_city", "LA");
        record.put("billing_city", "NYC");
        record.put("shipping_country", "USA");
        record.put("billing_country", "USA");
        record.put("payment_method", "card");
        record.put("card_last_digits", "1234");
        record.put("card_expiry", "12/27");
        record.put("ip_address", "10.0.0.1");
        record.put("user_agent", "JUnit");
        record.put("campaign_id", "CAMP123");
        record.put("referrer_url", "http://test.local");
        record.put("device_type", "mobile");
        record.put("browser", "chrome");
        record.put("os", "android");
        record.put("coupon_code", "DISCOUNT");
        record.put("discount_amount", 15.0);
        record.put("loyalty_points_used", 20);
        record.put("gift_wrap", true);
        record.put("special_instructions", "None");

        kafkaClientFactory.createProducer(true)
                .send(new ProducerRecord<>("order-events-test", null, record)).get();

        // Запуск ETL job
        EtlJob job = new EtlJob(
                "avro-to-sql",
                null,
                "SUPPORT.dbo.order_table",
                Map.of(
                        "extractorType", "kafka",
                        "format", "avro",
                        "transformerType", "avro",
                        "loaderType", "fast-sql",
                        "topic", "order-events-test",
                        "startTimestamp", Instant.now().minusSeconds(600).toEpochMilli(),
                        "endTimestamp", Instant.now().toEpochMilli(),
                        "batchSize", 100_000,
                        "threads", 4
                )
        );

        pipelineFactory.create(job).run(job);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM SUPPORT.dbo.order_table");
        assertThat(results).isNotEmpty();
        System.out.println("✅ Записей загружено: " + results.size());
    }


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

}
