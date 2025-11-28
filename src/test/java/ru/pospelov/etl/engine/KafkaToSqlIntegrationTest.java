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
import java.util.ArrayList;
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
                    null,      // ключ
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

    @Test
    @SneakyThrows
    void sqlToKafkaToSql_shouldTransferMillion() {
        jdbcTemplate.execute("DROP TABLE IF EXISTS SUPPORT.dbo.order_table_src");
        jdbcTemplate.execute("DROP TABLE IF EXISTS SUPPORT.dbo.order_table_dst");

        jdbcTemplate.execute("CREATE TABLE SUPPORT.dbo.order_table_src ("
                + "order_id NVARCHAR(64),"
                + "customer_id NVARCHAR(64),"
                + "order_date NVARCHAR(32),"
                + "delivery_date NVARCHAR(32),"
                + "status NVARCHAR(32),"
                + "total_amount FLOAT,"
                + "currency NVARCHAR(8),"
                + "item_count INT,"
                + "shipping_address NVARCHAR(800),"
                + "billing_address NVARCHAR(800),"
                + "shipping_zip NVARCHAR(16),"
                + "billing_zip NVARCHAR(16),"
                + "shipping_city NVARCHAR(64),"
                + "billing_city NVARCHAR(64),"
                + "shipping_country NVARCHAR(64),"
                + "billing_country NVARCHAR(64),"
                + "payment_method NVARCHAR(32),"
                + "card_last_digits NVARCHAR(8),"
                + "card_expiry NVARCHAR(16),"
                + "ip_address NVARCHAR(64),"
                + "user_agent NVARCHAR(800),"
                + "campaign_id NVARCHAR(64),"
                + "referrer_url NVARCHAR(800),"
                + "device_type NVARCHAR(32),"
                + "browser NVARCHAR(32),"
                + "os NVARCHAR(32),"
                + "coupon_code NVARCHAR(32),"
                + "discount_amount FLOAT,"
                + "loyalty_points_used INT,"
                + "gift_wrap BIT,"
                + "special_instructions NVARCHAR(800))");

        jdbcTemplate.execute("CREATE TABLE SUPPORT.dbo.order_table_dst ("
                + "order_id NVARCHAR(64),"
                + "customer_id NVARCHAR(64),"
                + "order_date NVARCHAR(32),"
                + "delivery_date NVARCHAR(32),"
                + "status NVARCHAR(32),"
                + "total_amount FLOAT,"
                + "currency NVARCHAR(8),"
                + "item_count INT,"
                + "shipping_address NVARCHAR(800),"
                + "billing_address NVARCHAR(800),"
                + "shipping_zip NVARCHAR(16),"
                + "billing_zip NVARCHAR(16),"
                + "shipping_city NVARCHAR(64),"
                + "billing_city NVARCHAR(64),"
                + "shipping_country NVARCHAR(64),"
                + "billing_country NVARCHAR(64),"
                + "payment_method NVARCHAR(32),"
                + "card_last_digits NVARCHAR(8),"
                + "card_expiry NVARCHAR(16),"
                + "ip_address NVARCHAR(64),"
                + "user_agent NVARCHAR(800),"
                + "campaign_id NVARCHAR(64),"
                + "referrer_url NVARCHAR(800),"
                + "device_type NVARCHAR(32),"
                + "browser NVARCHAR(32),"
                + "os NVARCHAR(32),"
                + "coupon_code NVARCHAR(32),"
                + "discount_amount FLOAT,"
                + "loyalty_points_used INT,"
                + "gift_wrap BIT,"
                + "special_instructions NVARCHAR(800))");

        String insertSql = "INSERT INTO SUPPORT.dbo.order_table_src " +
                "(order_id, customer_id, order_date, delivery_date, status, total_amount, currency, item_count, " +
                "shipping_address, billing_address, shipping_zip, billing_zip, shipping_city, billing_city, " +
                "shipping_country, billing_country, payment_method, card_last_digits, card_expiry, ip_address, " +
                "user_agent, campaign_id, referrer_url, device_type, browser, os, coupon_code, discount_amount, " +
                "loyalty_points_used, gift_wrap, special_instructions) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        int batchSize = 1000;
        List<Object[]> batchArgs = new ArrayList<>(batchSize);

        for (int i = 0; i < 1_000_000; i++) {
            Object[] params = new Object[]{
                    "ORD-" + i,
                    "CUST-" + i,
                    "2025-06-09",
                    "2025-06-10",
                    "PAID",
                    250.0,
                    "USD",
                    3,
                    "123 Avro Lane",
                    "456 Json Blvd",
                    "90210",
                    "10001",
                    "LA",
                    "NYC",
                    "USA",
                    "USA",
                    "card",
                    "1234",
                    "12/27",
                    "10.0.0.1",
                    "JUnit",
                    "CAMP123",
                    "http://test.local",
                    "mobile",
                    "chrome",
                    "android",
                    "DISCOUNT",
                    15.0,
                    20,
                    1,
                    "None"
            };

            batchArgs.add(params);

            if (batchArgs.size() == batchSize) {
                jdbcTemplate.batchUpdate(insertSql, batchArgs);
                batchArgs.clear(); // очищаем список для следующей порции
            }
        }

        // если остались "хвостовые" записи
        if (!batchArgs.isEmpty()) {
            jdbcTemplate.batchUpdate(insertSql, batchArgs);
        }


        Schema schema = fetchSchemaFromRegistry("order-events-value");

        EtlJob dump = new EtlJob(
                "sql-to-kafka",
                "SELECT * FROM SUPPORT.dbo.order_table_src",
                null,
                Map.of(
                        "extractorType", "jdbc",
                        "transformerType", "record-to-avro",
                        "loaderType", "kafka",
                        "format", "avro",
                        "topic", "order-events-bulk",
                        "threads", 8,
                        "batchSize", 100_000,
                        "avroSchema", schema
                )
        );

        pipelineFactory.create(dump).run(dump);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT COUNT(*) cnt FROM SUPPORT.dbo.order_table_src");
        int count = ((Number) results.get(0).get("cnt")).intValue();
        assertThat(count).isEqualTo(1_000_000);

        EtlJob load = new EtlJob(
                "bulk-avro-to-sql",
                null,
                "SUPPORT.dbo.order_table_dst",
                Map.of(
                        "extractorType", "kafka",
                        "format", "avro",
                        "transformerType", "avro",
                        "loaderType", "fast-sql",
                        "topic", "order-events-bulk",
                        "startTimestamp", Instant.now().minusSeconds(600).toEpochMilli(),
                        "endTimestamp", Instant.now().toEpochMilli(),
                        "batchSize", 100_000,
                        "threads", 8
                )
        );

        pipelineFactory.create(load).run(load);

        results = jdbcTemplate.queryForList("SELECT COUNT(*) cnt FROM SUPPORT.dbo.order_table_dst");
        count = ((Number) results.get(0).get("cnt")).intValue();
        assertThat(count).isEqualTo(1_000_000);
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

    @Test
    void sqlTableToKafkaAndBack_shouldTransferMillionRows() {
        jdbcTemplate.execute("IF OBJECT_ID('SUPPORT.dbo.order_src', 'U') IS NOT NULL DROP TABLE SUPPORT.dbo.order_src");
        jdbcTemplate.execute("IF OBJECT_ID('SUPPORT.dbo.order_dst', 'U') IS NOT NULL DROP TABLE SUPPORT.dbo.order_dst");

        jdbcTemplate.execute("IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name='ps_bucket') DROP PARTITION SCHEME ps_bucket");
        jdbcTemplate.execute("IF EXISTS (SELECT * FROM sys.partition_functions WHERE name='pf_bucket') DROP PARTITION FUNCTION pf_bucket");

        StringBuilder pf = new StringBuilder(1024);
        pf.append("CREATE PARTITION FUNCTION pf_bucket(int) AS RANGE LEFT FOR VALUES (0");
        for (int i = 1; i < 72; i++) pf.append(',').append(i);
        pf.append(')');
        jdbcTemplate.execute(pf.toString());
        jdbcTemplate.execute("CREATE PARTITION SCHEME ps_bucket AS PARTITION pf_bucket ALL TO ([PRIMARY])");

        jdbcTemplate.execute("CREATE TABLE SUPPORT.dbo.order_src (" +
                "order_id NVARCHAR(64)," +
                "customer_id NVARCHAR(64)," +
                "order_date NVARCHAR(32)," +
                "delivery_date NVARCHAR(32)," +
                "status NVARCHAR(32)," +
                "total_amount FLOAT," +
                "currency NVARCHAR(8)," +
                "item_count INT," +
                "shipping_address NVARCHAR(800)," +
                "billing_address NVARCHAR(800)," +
                "shipping_zip NVARCHAR(16)," +
                "billing_zip NVARCHAR(16)," +
                "shipping_city NVARCHAR(64)," +
                "billing_city NVARCHAR(64)," +
                "shipping_country NVARCHAR(64)," +
                "billing_country NVARCHAR(64)," +
                "payment_method NVARCHAR(32)," +
                "card_last_digits NVARCHAR(8)," +
                "card_expiry NVARCHAR(16)," +
                "ip_address NVARCHAR(64)," +
                "user_agent NVARCHAR(800)," +
                "campaign_id NVARCHAR(64)," +
                "referrer_url NVARCHAR(800)," +
                "device_type NVARCHAR(32)," +
                "browser NVARCHAR(32)," +
                "os NVARCHAR(32)," +
                "coupon_code NVARCHAR(32)," +
                "discount_amount FLOAT," +
                "loyalty_points_used INT," +
                "gift_wrap BIT," +
                "special_instructions NVARCHAR(800)," +
                "bucket AS (ABS(CHECKSUM(order_id)) % 72) PERSISTED" +
                ") ON ps_bucket(bucket)");

        jdbcTemplate.execute("CREATE TABLE SUPPORT.dbo.order_dst (" +
                "order_id NVARCHAR(64)," +
                "customer_id NVARCHAR(64)," +
                "order_date NVARCHAR(32)," +
                "delivery_date NVARCHAR(32)," +
                "status NVARCHAR(32)," +
                "total_amount FLOAT," +
                "currency NVARCHAR(8)," +
                "item_count INT," +
                "shipping_address NVARCHAR(800)," +
                "billing_address NVARCHAR(800)," +
                "shipping_zip NVARCHAR(16)," +
                "billing_zip NVARCHAR(16)," +
                "shipping_city NVARCHAR(64)," +
                "billing_city NVARCHAR(64)," +
                "shipping_country NVARCHAR(64)," +
                "billing_country NVARCHAR(64)," +
                "payment_method NVARCHAR(32)," +
                "card_last_digits NVARCHAR(8)," +
                "card_expiry NVARCHAR(16)," +
                "ip_address NVARCHAR(64)," +
                "user_agent NVARCHAR(800)," +
                "campaign_id NVARCHAR(64)," +
                "referrer_url NVARCHAR(800)," +
                "device_type NVARCHAR(32)," +
                "browser NVARCHAR(32)," +
                "os NVARCHAR(32)," +
                "coupon_code NVARCHAR(32)," +
                "discount_amount FLOAT," +
                "loyalty_points_used INT," +
                "gift_wrap BIT," +
                "special_instructions NVARCHAR(800)," +
                "bucket AS (ABS(CHECKSUM(order_id)) % 72) PERSISTED" +
                ") ON ps_bucket(bucket)");

        String insertSql = "INSERT INTO SUPPORT.dbo.order_src " +
                "(order_id, customer_id, order_date, delivery_date, status, total_amount, currency, item_count, " +
                "shipping_address, billing_address, shipping_zip, billing_zip, shipping_city, billing_city, shipping_country, billing_country, " +
                "payment_method, card_last_digits, card_expiry, ip_address, user_agent, campaign_id, referrer_url, device_type, browser, os, " +
                "coupon_code, discount_amount, loyalty_points_used, gift_wrap, special_instructions) " +
                "VALUES (?, 'CUST', '2025-06-09', '2025-06-10', 'PAID', 250.0, 'USD', 3, '123 Avro Lane', '456 Json Blvd', '90210', '10001', 'LA', 'NYC', 'USA', 'USA', " +
                "'card', '1234', '12/27', '10.0.0.1', 'JUnit', 'CAMP123', 'http://test.local', 'mobile', 'chrome', 'android', 'DISCOUNT', 15.0, 20, 1, 'None')";

        int batchSize = 1_000;
        List<Object[]> params = new ArrayList<>(batchSize);
        for (int i = 1; i <= 1_000_000; i++) {
            params.add(new Object[]{"ORD-" + i});
            if (params.size() == batchSize) {
                jdbcTemplate.batchUpdate(insertSql, params);
                params.clear();
            }
        }
        if (!params.isEmpty()) {
            jdbcTemplate.batchUpdate(insertSql, params);
        }

        Schema schema = null;
        try {
            schema = fetchSchemaFromRegistry("order-events-value");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        EtlJob toKafka = new EtlJob(
                "sql-to-kafka",
                "SELECT * FROM SUPPORT.dbo.order_src",
                null,
                Map.ofEntries(
                        Map.entry("extractorType", "sql"),
                        Map.entry("loaderType", "kafka"),
                        Map.entry("transformerType", "noop"),
                        Map.entry("topic", "order-events-test"),
                        Map.entry("format", "avro"),
                        Map.entry("threads", 8),
                        Map.entry("batchSize", 100_000),
                        Map.entry("avroSchema", schema.toString()),
                        Map.entry("keyColumn", "order_id"),
                        Map.entry("partitionColumn", "bucket"),
                        Map.entry("partitions", 72)
                )
        );

        pipelineFactory.create(toKafka).run(toKafka);

        EtlJob fromKafka = new EtlJob(
                "avro-to-sql",
                null,
                "SUPPORT.dbo.order_dst",
                Map.of(
                        "extractorType", "kafka",
                        "format", "avro",
                        "transformerType", "avro",
                        "loaderType", "fast-sql",
                        "topic", "order-events-test",
                        "startTimestamp", Instant.now().minusSeconds(600).toEpochMilli(),
                        "endTimestamp", Instant.now().toEpochMilli(),
                        "batchSize", 100_000,
                        "threads", 8
                )
        );

        pipelineFactory.create(fromKafka).run(fromKafka);

        Integer cnt = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM SUPPORT.dbo.order_dst", Integer.class);
        assertThat(cnt).isEqualTo(10_000);
    }


    @Test
    void sqlTableToKafka_shouldTransferMillionRows() {
        jdbcTemplate.execute("IF OBJECT_ID('SUPPORT.dbo.order_src', 'U') IS NOT NULL DROP TABLE SUPPORT.dbo.order_src");
        jdbcTemplate.execute("IF OBJECT_ID('SUPPORT.dbo.order_dst', 'U') IS NOT NULL DROP TABLE SUPPORT.dbo.order_dst");

        jdbcTemplate.execute("IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name='ps_bucket') DROP PARTITION SCHEME ps_bucket");
        jdbcTemplate.execute("IF EXISTS (SELECT * FROM sys.partition_functions WHERE name='pf_bucket') DROP PARTITION FUNCTION pf_bucket");

        StringBuilder pf = new StringBuilder(1024);
        pf.append("CREATE PARTITION FUNCTION pf_bucket(int) AS RANGE LEFT FOR VALUES (0");
        for (int i = 1; i < 72; i++) pf.append(',').append(i);
        pf.append(')');
        jdbcTemplate.execute(pf.toString());
        jdbcTemplate.execute("CREATE PARTITION SCHEME ps_bucket AS PARTITION pf_bucket ALL TO ([PRIMARY])");

        jdbcTemplate.execute("CREATE TABLE SUPPORT.dbo.order_src (" +
                "order_id NVARCHAR(64)," +
                "customer_id NVARCHAR(64)," +
                "order_date NVARCHAR(32)," +
                "delivery_date NVARCHAR(32)," +
                "status NVARCHAR(32)," +
                "total_amount FLOAT," +
                "currency NVARCHAR(8)," +
                "item_count INT," +
                "shipping_address NVARCHAR(800)," +
                "billing_address NVARCHAR(800)," +
                "shipping_zip NVARCHAR(16)," +
                "billing_zip NVARCHAR(16)," +
                "shipping_city NVARCHAR(64)," +
                "billing_city NVARCHAR(64)," +
                "shipping_country NVARCHAR(64)," +
                "billing_country NVARCHAR(64)," +
                "payment_method NVARCHAR(32)," +
                "card_last_digits NVARCHAR(8)," +
                "card_expiry NVARCHAR(16)," +
                "ip_address NVARCHAR(64)," +
                "user_agent NVARCHAR(800)," +
                "campaign_id NVARCHAR(64)," +
                "referrer_url NVARCHAR(800)," +
                "device_type NVARCHAR(32)," +
                "browser NVARCHAR(32)," +
                "os NVARCHAR(32)," +
                "coupon_code NVARCHAR(32)," +
                "discount_amount FLOAT," +
                "loyalty_points_used INT," +
                "gift_wrap BIT," +
                "special_instructions NVARCHAR(800)," +
                "bucket AS (ABS(CHECKSUM(order_id)) % 72) PERSISTED" +
                ") ON ps_bucket(bucket)");

        jdbcTemplate.execute("CREATE TABLE SUPPORT.dbo.order_dst (" +
                "order_id NVARCHAR(64)," +
                "customer_id NVARCHAR(64)," +
                "order_date NVARCHAR(32)," +
                "delivery_date NVARCHAR(32)," +
                "status NVARCHAR(32)," +
                "total_amount FLOAT," +
                "currency NVARCHAR(8)," +
                "item_count INT," +
                "shipping_address NVARCHAR(800)," +
                "billing_address NVARCHAR(800)," +
                "shipping_zip NVARCHAR(16)," +
                "billing_zip NVARCHAR(16)," +
                "shipping_city NVARCHAR(64)," +
                "billing_city NVARCHAR(64)," +
                "shipping_country NVARCHAR(64)," +
                "billing_country NVARCHAR(64)," +
                "payment_method NVARCHAR(32)," +
                "card_last_digits NVARCHAR(8)," +
                "card_expiry NVARCHAR(16)," +
                "ip_address NVARCHAR(64)," +
                "user_agent NVARCHAR(800)," +
                "campaign_id NVARCHAR(64)," +
                "referrer_url NVARCHAR(800)," +
                "device_type NVARCHAR(32)," +
                "browser NVARCHAR(32)," +
                "os NVARCHAR(32)," +
                "coupon_code NVARCHAR(32)," +
                "discount_amount FLOAT," +
                "loyalty_points_used INT," +
                "gift_wrap BIT," +
                "special_instructions NVARCHAR(800)," +
                "bucket AS (ABS(CHECKSUM(order_id)) % 72) PERSISTED" +
                ") ON ps_bucket(bucket)");

        String insertSql = "INSERT INTO SUPPORT.dbo.order_src " +
                "(order_id, customer_id, order_date, delivery_date, status, total_amount, currency, item_count, " +
                "shipping_address, billing_address, shipping_zip, billing_zip, shipping_city, billing_city, shipping_country, billing_country, " +
                "payment_method, card_last_digits, card_expiry, ip_address, user_agent, campaign_id, referrer_url, device_type, browser, os, " +
                "coupon_code, discount_amount, loyalty_points_used, gift_wrap, special_instructions) " +
                "VALUES (?, 'CUST', '2025-06-09', '2025-06-10', 'PAID', 250.0, 'USD', 3, '123 Avro Lane', '456 Json Blvd', '90210', '10001', 'LA', 'NYC', 'USA', 'USA', " +
                "'card', '1234', '12/27', '10.0.0.1', 'JUnit', 'CAMP123', 'http://test.local', 'mobile', 'chrome', 'android', 'DISCOUNT', 15.0, 20, 1, 'None')";

        int batchSize = 100_000;
        List<Object[]> params = new ArrayList<>(batchSize);
        for (int i = 1; i <= 1_000_000; i++) {
            params.add(new Object[]{"ORD-" + i});
            if (params.size() == batchSize) {
                jdbcTemplate.batchUpdate(insertSql, params);
                params.clear();
            }
        }
        if (!params.isEmpty()) {
            jdbcTemplate.batchUpdate(insertSql, params);
        }

        Schema schema = null;
        try {
            schema = fetchSchemaFromRegistry("order-events-value");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        EtlJob toKafka = new EtlJob(
                "sql-to-kafka",
                "SELECT * FROM SUPPORT.dbo.order_src",
                null,
                Map.ofEntries(
                        Map.entry("extractorType", "sql"),
                        Map.entry("loaderType", "kafka"),
                        Map.entry("transformerType", "noop"),
                        Map.entry("topic", "order-events-value"),
                        Map.entry("format", "avro"),
                        Map.entry("threads", 8),
                        Map.entry("batchSize", 100_000),
                        Map.entry("avroSchema", schema.toString()),
                        Map.entry("keyColumn", "order_id"),
                        Map.entry("partitionColumn", "bucket"),
                        Map.entry("partitions", 72)
                )
        );

        pipelineFactory.create(toKafka).run(toKafka);
    }
}
