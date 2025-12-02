package ru.pospelov.etl.engine;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.pospelov.etl.engine.engine.EtlPipelineFactory;
import ru.pospelov.etl.engine.model.EtlBulkRecord;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.config.KafkaClientFactory;
import ru.pospelov.etl.engine.model.EtlRecord;

import javax.sql.DataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class KafkaToSqlIntegrationTest {

    @Autowired
    private EtlPipelineFactory pipelineFactory;

    @Autowired
    private KafkaClientFactory kafkaClientFactory;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Оптимизированная версия теста sqlTableToKafka_shouldTransferMillionRows.
     * 
     * Подготовка данных:
     * 1. Создает временную таблицу БЕЗ партиционирования (быстрее заполняется)
     * 2. Генерирует данные в памяти как EtlRecord
     * 3. Загружает данные во временную таблицу через SQL Server Bulk Copy API (намного быстрее чем batch INSERT)
     * 4. Копирует данные из временной таблицы в целевую партиционированную таблицу через INSERT ... SELECT
     * 
     * Streaming ETL обработка (новая архитектура):
     * 5. Использует батчевую обработку (streamBatchSize) для экономии памяти
     * 6. Данные обрабатываются порциями: extract batch → transform → load → next batch
     * 7. Вместо 2M записей в памяти одновременно - только текущий батч от каждого потока
     * 
     * Этот подход позволяет загрузить 2 000 000 строк за несколько секунд с минимальным потреблением памяти.
     */
    @Test
    @SneakyThrows
    void sqlTableToKafka_shouldTransferMillionRows_Optimized() {
        jdbcTemplate.execute("IF OBJECT_ID('SUPPORT.dbo.order_src', 'U') IS NOT NULL DROP TABLE SUPPORT.dbo.order_src");
        jdbcTemplate.execute("IF OBJECT_ID('SUPPORT.dbo.order_dst', 'U') IS NOT NULL DROP TABLE SUPPORT.dbo.order_dst");
        jdbcTemplate.execute("IF OBJECT_ID('SUPPORT.dbo.order_src_temp', 'U') IS NOT NULL DROP TABLE SUPPORT.dbo.order_src_temp");

        jdbcTemplate.execute("IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name='ps_bucket') DROP PARTITION SCHEME ps_bucket");
        jdbcTemplate.execute("IF EXISTS (SELECT * FROM sys.partition_functions WHERE name='pf_bucket') DROP PARTITION FUNCTION pf_bucket");

        StringBuilder pf = new StringBuilder();
        pf.append("CREATE PARTITION FUNCTION pf_bucket(int) AS RANGE LEFT FOR VALUES (0");
        for (int i = 1; i < 72; i++) pf.append(',').append(i);
        pf.append(')');
        jdbcTemplate.execute(pf.toString());
        jdbcTemplate.execute("CREATE PARTITION SCHEME ps_bucket AS PARTITION pf_bucket ALL TO ([PRIMARY])");

        // Создаем целевую партиционированную таблицу
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

        // Создаем временную таблицу БЕЗ партиционирования для быстрой загрузки
        jdbcTemplate.execute("CREATE TABLE SUPPORT.dbo.order_src_temp (" +
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
                "special_instructions NVARCHAR(800)" +
                ")");

        // Генерируем данные в памяти и загружаем через Bulk Copy
        System.out.println("Генерация данных в памяти...");
        List<EtlRecord> records = new ArrayList<>(8_000_000);
        Instant now = Instant.now();
        for (int i = 1; i <= 8_000_000; i++) {
            EtlRecord record = new EtlRecord(
                    now, "test", i
            );
            record.put("order_id", "ORD-" + i);
            record.put("customer_id", "CUST");
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
            record.put("gift_wrap", 1); // BIT в SQL Server
            record.put("special_instructions", "None");
            records.add(record);
        }

        // Загружаем данные во временную таблицу через Bulk Copy
        System.out.println("Загрузка данных во временную таблицу через Bulk Copy...");
        Instant loadStart = Instant.now();
        DataSource dataSource = jdbcTemplate.getDataSource();
        if (dataSource == null) {
            throw new RuntimeException("DataSource is null");
        }

        try (Connection connection = dataSource.getConnection()) {
            SQLServerConnection sqlConn = connection.unwrap(SQLServerConnection.class);
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlConn)) {
                bulkCopy.setDestinationTableName("SUPPORT.dbo.order_src_temp");

                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setTableLock(true);
                options.setCheckConstraints(false);
                options.setFireTriggers(false);
                options.setKeepNulls(true);
                options.setBatchSize(100_000); // Большой размер батча для Bulk Copy
                bulkCopy.setBulkCopyOptions(options);

                // Добавляем маппинг колонок
                for (String column : records.get(0).getAll().keySet()) {
                    bulkCopy.addColumnMapping(column, column);
                }

                bulkCopy.writeToServer(new EtlBulkRecord(records));
            }
        }
        Instant loadEnd = Instant.now();
        long loadDurationSeconds = Duration.between(loadStart, loadEnd).toSeconds();
        long loadRowsPerSecond = loadDurationSeconds > 0 ? records.size() / loadDurationSeconds : records.size();
        System.out.println("✅ Загрузка во временную таблицу завершена за " + loadDurationSeconds + 
                " секунд (" + String.format("%,d", loadRowsPerSecond) + " строк/сек)");

        // Копируем данные из временной таблицы в целевую партиционированную таблицу
        System.out.println("Копирование данных в партиционированную таблицу...");
        Instant copyStart = Instant.now();
        jdbcTemplate.execute("INSERT INTO SUPPORT.dbo.order_src " +
                "(order_id, customer_id, order_date, delivery_date, status, total_amount, currency, item_count, " +
                "shipping_address, billing_address, shipping_zip, billing_zip, shipping_city, billing_city, " +
                "shipping_country, billing_country, payment_method, card_last_digits, card_expiry, ip_address, " +
                "user_agent, campaign_id, referrer_url, device_type, browser, os, coupon_code, discount_amount, " +
                "loyalty_points_used, gift_wrap, special_instructions) " +
                "SELECT " +
                "order_id, customer_id, order_date, delivery_date, status, total_amount, currency, item_count, " +
                "shipping_address, billing_address, shipping_zip, billing_zip, shipping_city, billing_city, " +
                "shipping_country, billing_country, payment_method, card_last_digits, card_expiry, ip_address, " +
                "user_agent, campaign_id, referrer_url, device_type, browser, os, coupon_code, discount_amount, " +
                "loyalty_points_used, gift_wrap, special_instructions " +
                "FROM SUPPORT.dbo.order_src_temp");
        Instant copyEnd = Instant.now();
        long copyDurationSeconds = Duration.between(copyStart, copyEnd).toSeconds();
        long copyRowsPerSecond = copyDurationSeconds > 0 ? records.size() / copyDurationSeconds : records.size();
        System.out.println("✅ Копирование в партиционированную таблицу завершено за " + copyDurationSeconds + 
                " секунд (" + String.format("%,d", copyRowsPerSecond) + " строк/сек)");

        // Удаляем временную таблицу
        jdbcTemplate.execute("DROP TABLE SUPPORT.dbo.order_src_temp");

        // Проверяем количество записей
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM SUPPORT.dbo.order_src", Integer.class);
        assertThat(count).isEqualTo(8_000_000);
        System.out.println("✅ Проверка: загружено " + count + " записей");

        // Продолжаем с тестом передачи в Kafka
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
                        // Streaming batch size - размер батча для потоковой обработки
                        // Вместо загрузки всех 2M записей в память, обрабатываются батчами по 100K
                        Map.entry("streamBatchSize", 100_000),
                        Map.entry("avroSchema", schema.toString()),
                        Map.entry("keyColumn", "order_id"),
                        // Партиционирование SQL: каждый поток обрабатывает свои партиции
                        Map.entry("partitionColumn", "bucket"),
                        Map.entry("partitions", 72)
                )
        );

        // Замер времени загрузки в Kafka
        System.out.println("Начало загрузки данных в Kafka...");
        Instant kafkaStart = Instant.now();
        pipelineFactory.create(toKafka).run(toKafka);
        Instant kafkaEnd = Instant.now();
        long kafkaDurationSeconds = Duration.between(kafkaStart, kafkaEnd).toSeconds();
        long kafkaRowsPerSecond = kafkaDurationSeconds > 0 ? count / kafkaDurationSeconds : count;
        System.out.println("✅ Загрузка в Kafka завершена за " + kafkaDurationSeconds + 
                " секунд (" + String.format("%,d", kafkaRowsPerSecond) + " строк/сек)");

        // Обратная загрузка из Kafka в order_dst
        System.out.println("Начало загрузки данных из Kafka в order_dst...");
        Instant kafkaToSqlStart = Instant.now();
        EtlJob fromKafka = new EtlJob(
                "kafka-to-sql",
                null,
                "SUPPORT.dbo.order_dst",
                Map.of(
                        "extractorType", "kafka",
                        "format", "avro",
                        "transformerType", "avro",
                        "loaderType", "fast-sql",
                        "topic", "order-events-value",
                        "startTimestamp", kafkaStart.minusSeconds(60).toEpochMilli(), // Начинаем немного раньше для надежности
                        "endTimestamp", kafkaEnd.plusSeconds(60).toEpochMilli(),     // Заканчиваем немного позже для надежности
                        // Streaming batch size - единый размер батча для всей обработки
                        "streamBatchSize", 100_000,
                        "threads", 8
                )
        );
        pipelineFactory.create(fromKafka).run(fromKafka);
        Instant kafkaToSqlEnd = Instant.now();
        long kafkaToSqlDurationSeconds = Duration.between(kafkaToSqlStart, kafkaToSqlEnd).toSeconds();
        
        // Проверяем количество загруженных записей
        Integer dstCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM SUPPORT.dbo.order_dst", Integer.class);
        long kafkaToSqlRowsPerSecond = kafkaToSqlDurationSeconds > 0 ? dstCount / kafkaToSqlDurationSeconds : dstCount;
        System.out.println("✅ Загрузка из Kafka в order_dst завершена за " + kafkaToSqlDurationSeconds + 
                " секунд (" + String.format("%,d", kafkaToSqlRowsPerSecond) + " строк/сек)");
        System.out.println("✅ Проверка: загружено " + dstCount + " записей в order_dst");
        assertThat(dstCount).isEqualTo(count);
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
