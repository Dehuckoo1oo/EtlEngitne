package ru.pospelov.etl.engine.steps.loader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.KafkaClientFactory;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
            List<PendingSend> pendingSends = new ArrayList<>(records.size());
            for (EtlRecord record : records) {
                try {
                    ProducerRecord<String, Object> kafkaRecord = new ProducerRecord<>(
                            topic,
                            record.get("key") == null ? null : record.get("key").toString(),
                            record.get("value")
                    );
                    Future<RecordMetadata> future = producer.send(kafkaRecord);
                    pendingSends.add(new PendingSend(record, future));
                } catch (Exception e) {
                    throw new LoadingException("Kafka loading failed", job.getJobId(), record, EtlErrorSeverity.CRITICAL, e);
                }
            }
            producer.flush();
            waitForSends(job, pendingSends);
        } catch (LoadingException e) {
            throw e;
        } catch (Exception e) {
            log.error("Kafka load failed", e);
            throw new LoadingException("Kafka load failed", job.getJobId(), null, EtlErrorSeverity.CRITICAL, e);
        }
    }

    private void waitForSends(EtlJob job, List<PendingSend> pendingSends) {
        for (PendingSend pending : pendingSends) {
            try {
                pending.future().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LoadingException("Kafka loading interrupted", job.getJobId(), pending.record(), EtlErrorSeverity.CRITICAL, e);
            } catch (ExecutionException e) {
                throw new LoadingException("Kafka loading failed", job.getJobId(), pending.record(), EtlErrorSeverity.CRITICAL, e.getCause());
            }
        }
    }

    private record PendingSend(EtlRecord record, Future<RecordMetadata> future) {}
}