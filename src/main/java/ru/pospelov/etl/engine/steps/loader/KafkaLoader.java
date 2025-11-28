package ru.pospelov.etl.engine.steps.loader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.KafkaClientFactory;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaLoader implements Loader {

    private final KafkaClientFactory clientFactory;

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

        try (Producer<String, Object> producer = clientFactory.createProducer(isAvro)) {
            for (EtlRecord r : records) {
                producer.send(new ProducerRecord<>(topic,
                        r.get("key") == null ? null : r.get("key").toString(),
                        r.get("value")));
            }
            producer.flush();
        } catch (Exception e) {
            log.error("Kafka load failed", e);
            throw new RuntimeException(e);
        }
    }
}