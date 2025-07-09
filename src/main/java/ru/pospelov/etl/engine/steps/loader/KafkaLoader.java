package ru.pospelov.etl.engine.steps.loader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.KafkaClientFactory;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaLoader implements Loader {

    private final KafkaClientFactory kafkaClientFactory;

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public void load(Collection<EtlRecord> records, EtlJob job) {
        if (records.isEmpty()) return;

        String topic = job.getParam("topic").toString();
        String format = String.valueOf(job.getParamOrDefault("format", "string"));
        boolean isAvro = format.equalsIgnoreCase("avro");
        int threadCount = (int) job.getParamOrDefault("threads", 4);
        List<EtlRecord> recordList = new ArrayList<>(records);

        try (Producer<String, Object> producer = kafkaClientFactory.createProducer(isAvro)) {
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            for (EtlRecord r : recordList) {
                executor.submit(() -> {
                    Object value = r.get("value");
                    String key = (String) r.get("key");
                    try {
                        producer.send(new ProducerRecord<>(topic, key, value)).get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                    log.warn("Kafka load executor did not finish in time");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}