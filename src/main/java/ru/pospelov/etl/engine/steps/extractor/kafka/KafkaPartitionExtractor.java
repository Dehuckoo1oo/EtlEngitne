package ru.pospelov.etl.engine.steps.extractor.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.KafkaClientFactory;
import ru.pospelov.etl.engine.config.KafkaFormat;
import ru.pospelov.etl.engine.config.extractor.KafkaExtractorConfig;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.ExtractionException;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaPartitionExtractor {

    private final KafkaClientFactory consumerFactory;

    /**
     * Extract data from Kafka using type-safe configuration.
     */
    public void extract(
            KafkaExtractorConfig config,
            String jobId,
            Consumer<Collection<EtlRecord>> batchConsumer
    ) {
        // Type-safe access
        String topic = config.topic();
        long startMillis = config.startTimestamp();
        long endMillis = config.endTimestamp();
        int threadCount = config.threads();
        int streamBatchSize = config.streamBatchSize();
        KafkaFormat format = config.format();

        if (startMillis >= endMillis) {
            throw new ExtractionException("startTimestamp must be before endTimestamp", jobId);
        }

        boolean isAvro = (format == KafkaFormat.AVRO);

        log.info("Job '{}' Kafka extraction started: topic={}, startTimestamp={}, endTimestamp={}, threads={}, format={}",
                jobId, topic, Instant.ofEpochMilli(startMillis), Instant.ofEpochMilli(endMillis), threadCount, format);

        Map<String, Object> consumerProps = consumerFactory.buildConsumerConfig("kafka-extractor", isAvro);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> tasks = new ArrayList<>();

        try (KafkaConsumer<String, Object> metadataConsumer = new KafkaConsumer<>(consumerProps)) {
            List<PartitionInfo> partitions = metadataConsumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                log.warn("No partitions found for topic {}", topic);
                return;
            }

            for (PartitionInfo partition : partitions) {
                TopicPartition tp = new TopicPartition(partition.topic(), partition.partition());
                tasks.add(executor.submit(() ->
                        consumePartition(jobId, consumerProps, tp, startMillis, endMillis, streamBatchSize, batchConsumer)
                ));
            }

            waitForTasks(tasks, jobId);
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                throw new ExtractionException("Kafka extractor did not finish within timeout", jobId);
            }
            log.info("Job '{}' Kafka extraction completed: {} partitions processed", jobId, partitions.size());
        } catch (EtlException e) {
            log.error("Job '{}' Kafka extraction failed: {}", jobId, e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Job '{}' Kafka extraction interrupted", jobId);
            throw new ExtractionException("Kafka extraction interrupted", jobId, null, EtlErrorSeverity.CRITICAL, e);
        } catch (Exception e) {
            log.error("Job '{}' Kafka extraction error: {}", jobId, e.getMessage(), e);
            throw new ExtractionException("Kafka extraction failed", jobId, null, EtlErrorSeverity.CRITICAL, e);
        } finally {
            executor.shutdownNow();
        }
    }

    private void consumePartition(
            String jobId,
            Map<String, Object> consumerProps,
            TopicPartition tp,
            long startMillis,
            long endMillis,
            int streamBatchSize,
            Consumer<Collection<EtlRecord>> batchConsumer
    ) {
        try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.assign(List.of(tp));
            Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(Map.of(tp, startMillis));
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
            if (offsetAndTimestamp == null) {
                return;
            }

            consumer.seek(tp, offsetAndTimestamp.offset());
            List<EtlRecord> currentBatch = new ArrayList<>(streamBatchSize);

            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<String, Object> record : records.records(tp)) {
                    if (record.timestamp() > endMillis) {
                        // Send remaining batch before exiting
                        if (!currentBatch.isEmpty()) {
                            batchConsumer.accept(new ArrayList<>(currentBatch));
                        }
                        return;
                    }

                    EtlRecord etlRecord = new EtlRecord(
                            Instant.ofEpochMilli(record.timestamp()),
                            tp.toString(),
                            record.offset()
                    );
                    if (record.key() != null) {
                        etlRecord.put("key", record.key());
                    }
                    if (record.value() == null) {
                        // Skip tombstone/null payloads to avoid downstream NPEs
                        continue;
                    }
                    etlRecord.put("value", record.value());

                    currentBatch.add(etlRecord);

                    // When batch is full, send it for processing immediately
                    if (currentBatch.size() >= streamBatchSize) {
                        batchConsumer.accept(new ArrayList<>(currentBatch));
                        currentBatch.clear();
                    }
                }
            }

            // Send remaining records
            if (!currentBatch.isEmpty()) {
                batchConsumer.accept(new ArrayList<>(currentBatch));
            }
        } catch (Exception e) {
            throw new ExtractionException(
                    "Kafka extraction failed for partition " + tp,
                    jobId,
                    null,
                    EtlErrorSeverity.CRITICAL,
                    e
            );
        }
    }

    private static void waitForTasks(List<Future<?>> tasks, String jobId) throws InterruptedException {
        for (Future<?> task : tasks) {
            try {
                task.get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof EtlException etlException) {
                    throw etlException;
                }
                throw new ExtractionException("Kafka extraction task failed", jobId, null, EtlErrorSeverity.CRITICAL, cause);
            }
        }
    }
}
