package ru.pospelov.etl.engine.steps.loader;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.KafkaClientFactory;
import ru.pospelov.etl.engine.config.KafkaFormat;
import ru.pospelov.etl.engine.config.loader.KafkaLoaderConfig;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaLoader {

    private final KafkaClientFactory clientFactory;

    // Кэширование Producer'ов для переиспользования (дорогая операция создания)
    private volatile Producer<String, Object> avroProducer;
    private volatile Producer<String, Object> stringProducer;
    private final Object producerLock = new Object();

    /**
     * Load data to Kafka using type-safe configuration.
     */
    public void load(KafkaLoaderConfig config, String jobId, Collection<EtlRecord> records) {
        if (records.isEmpty()) return;

        String topic = config.topic();
        KafkaFormat format = config.format();
        boolean isAvro = (format == KafkaFormat.AVRO);

        try {
            // Используем кэшированный Producer вместо создания нового
            Producer<String, Object> producer = getProducer(isAvro);
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
                    throw new LoadingException("Kafka loading failed", jobId, record, EtlErrorSeverity.CRITICAL, e);
                }
            }

            producer.flush();
            waitForSends(jobId, pendingSends);
        } catch (LoadingException e) {
            throw e;
        } catch (Exception e) {
            log.error("Kafka load failed", e);
            throw new LoadingException("Kafka load failed", jobId, null, EtlErrorSeverity.CRITICAL, e);
        }
    }

    /**
     * Получить или создать кэшированный Kafka Producer.
     * Использует double-checked locking для потокобезопасности.
     *
     * @param isAvro true для Avro producer, false для String producer
     * @return кэшированный Producer
     */
    private Producer<String, Object> getProducer(boolean isAvro) {
        if (isAvro) {
            if (avroProducer == null) {
                synchronized (producerLock) {
                    if (avroProducer == null) {
                        log.info("Creating new Avro Kafka Producer (will be reused)");
                        avroProducer = clientFactory.createProducer(true);
                    }
                }
            }
            return avroProducer;
        } else {
            if (stringProducer == null) {
                synchronized (producerLock) {
                    if (stringProducer == null) {
                        log.info("Creating new String Kafka Producer (will be reused)");
                        stringProducer = clientFactory.createProducer(false);
                    }
                }
            }
            return stringProducer;
        }
    }

    /**
     * Закрыть кэшированные Producer'ы при остановке приложения.
     */
    @PreDestroy
    public void cleanup() {
        log.info("Closing cached Kafka Producers");
        if (avroProducer != null) {
            try {
                avroProducer.close();
            } catch (Exception e) {
                log.warn("Failed to close Avro producer", e);
            }
        }
        if (stringProducer != null) {
            try {
                stringProducer.close();
            } catch (Exception e) {
                log.warn("Failed to close String producer", e);
            }
        }
    }

    private void waitForSends(String jobId, List<PendingSend> pendingSends) {
        for (PendingSend pending : pendingSends) {
            try {
                pending.future().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LoadingException("Kafka loading interrupted", jobId, pending.record(), EtlErrorSeverity.CRITICAL, e);
            } catch (ExecutionException e) {
                throw new LoadingException("Kafka loading failed", jobId, pending.record(), EtlErrorSeverity.CRITICAL, e.getCause());
            }
        }
    }

    private record PendingSend(EtlRecord record, Future<RecordMetadata> future) {}
}
