package ru.pospelov.etl.engine.steps.loader;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.KafkaClientFactory;
import ru.pospelov.etl.engine.config.KafkaFormat;
import ru.pospelov.etl.engine.config.loader.KafkaLoaderConfig;
import ru.pospelov.etl.engine.conversion.TypeConverter;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlStage;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.exception.TypeConversionException;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaByPartitionLoader {

    private final KafkaClientFactory clientFactory;
    private final TypeConverter typeConverter;

    // Кэширование Producer'ов для переиспользования (дорогая операция создания)
    private volatile Producer<String, Object> avroProducer;
    private volatile Producer<String, Object> stringProducer;
    private final Object producerLock = new Object();

    /**
     * Load data to Kafka using type-safe configuration.
     *
     * <p>For AVRO format: performs type conversion using TypeConverter to ensure
     * Java types (LocalDate, Instant, BigDecimal, SqlVariantValue) are converted
     * to Avro physical types (int, long, ByteBuffer, String).
     *
     * <p>If __kafka_value contains a GenericRecord (from RecordToAvroTransformer),
     * rebuilds it with proper type conversion. This ensures Avro serialization succeeds.
     */
    public void load(KafkaLoaderConfig config, String jobId, EtlBatch batch) {
        if (batch.getRecords().isEmpty()) return;

        String topic = config.topic();
        KafkaFormat format = config.format();
        boolean isAvro = (format == KafkaFormat.AVRO);

        try {
            // Используем кэшированный Producer вместо создания нового
            Producer<String, Object> producer = getProducer(isAvro);
            List<PendingSend> pendingSends = new ArrayList<>(batch.getRecords().size());

            for (EtlRecord record : batch.getRecords()) {
                try {
                    // Use __kafka_key field (set by extractors to avoid conflict with user columns)
                    Object keyValue = record.get("__kafka_key");
                    String key = keyValue == null ? null : keyValue.toString();

                    // Use __kafka_value field (set by extractors to avoid conflict with user columns)
                    Object value = record.get("__kafka_value");

                    // For AVRO format: if value is GenericRecord, rebuild it with type conversion
                    if (isAvro && value instanceof GenericRecord) {
                        value = convertGenericRecord((GenericRecord) value, jobId);
                    }

                    ProducerRecord<String, Object> kafkaRecord = new ProducerRecord<>(
                            topic,
                            key,
                            value
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
     * Convert GenericRecord by applying TypeConverter to each field.
     *
     * <p>This method rebuilds the GenericRecord, converting Java types to Avro physical types:
     * <ul>
     * <li>LocalDate → int (epoch days)</li>
     * <li>Instant → long (epoch millis)</li>
     * <li>BigDecimal → ByteBuffer (Avro decimal)</li>
     * <li>SqlVariantValue → String (JSON)</li>
     * <li>byte[] → ByteBuffer</li>
     * </ul>
     *
     * @param source GenericRecord with raw Java values (from RecordToAvroTransformer)
     * @param jobId job identifier for error reporting
     * @return new GenericRecord with converted Avro-compatible values
     */
    private GenericRecord convertGenericRecord(GenericRecord source, String jobId) {
        Schema schema = source.getSchema();
        GenericRecord target = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            Object rawValue = source.get(field.name());
            if (rawValue == null) {
                target.put(field.name(), null);
                continue;
            }

            try {
                Object avroValue = typeConverter.convertToAvro(rawValue, field, jobId);
                target.put(field.name(), avroValue);
            } catch (TypeConversionException e) {
                // Add context to the error
                log.error("Type conversion failed for field '{}' in Kafka load: expected Avro type={}, actual Java type={}, value={}",
                        field.name(),
                        field.schema(),
                        rawValue.getClass().getName(),
                        truncateValue(rawValue));
                throw new LoadingException(
                        String.format("Type conversion failed for field '%s': %s", field.name(), e.getMessage()),
                        jobId,
                        null,
                        EtlErrorSeverity.CRITICAL,
                        e
                );
            }
        }

        return target;
    }

    /**
     * Truncate value for logging (prevent huge log entries).
     */
    private String truncateValue(Object value) {
        String str = String.valueOf(value);
        return str.length() > 100 ? str.substring(0, 100) + "..." : str;
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
