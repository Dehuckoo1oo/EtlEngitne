package ru.pospelov.etl.engine;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.pospelov.etl.engine.engine.EtlPipelineFactory;
import ru.pospelov.etl.engine.model.EtlJob;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class KafkaToSqlIntegrationTest {

    @Autowired
    private EtlPipelineFactory pipelineFactory;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void kafkaToSql_shouldExtractAndInsert() {
        // Очистим таблицу перед тестом
        jdbcTemplate.execute("DELETE FROM target_table");

        EtlJob job = new EtlJob(
                "kafka-to-sql",
                null,
                "target_table",
                Map.of(
                        "topic", "etl_test_topic",
                        "startTimestamp", Instant.now().minusSeconds(600).toEpochMilli(),
                        "endTimestamp", Instant.now().toEpochMilli(),
                        "extractorType", "kafka",
                        "loaderType", "sql",
                        "transformerType", "noop",
                        "batchSize", 500,
                        "threads", 3
                )
        );

        pipelineFactory.create(job).run(job);

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM target_table");
        assertThat(results).isNotEmpty();
        System.out.println("✅ Записей в таблице: " + results.size());
    }
}
