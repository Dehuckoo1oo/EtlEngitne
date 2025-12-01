package ru.pospelov.etl.engine.steps.extractor;

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
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.ExtractionException;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaExtractor implements Extractor {

    private final KafkaClientFactory consumerFactory;

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public Collection<EtlRecord> extract(EtlJob job) {
        String topic = Objects.toString(job.getParam("topic"), "");
        if (topic.isBlank()) {
            throw new ExtractionException("Kafka topic is required", job.getJobId());
        }

        long startMillis = requireTimestamp(job, "startTimestamp");
        long endMillis = requireTimestamp(job, "endTimestamp");
        if (startMillis >= endMillis) {
            throw new ExtractionException("startTimestamp must be before endTimestamp", job.getJobId());
        }

        int threadCount = ((Number) job.getParamOrDefault("threads", 4)).intValue();
        String format = Objects.toString(job.getParamOrDefault("format", "string"), "string");
        boolean isAvro = format.equalsIgnoreCase("avro");

        Map<String, Object> consumerProps = consumerFactory.buildConsumerConfig("kafka-extractor", isAvro);
        List<EtlRecord> allRecords = Collections.synchronizedList(new ArrayList<>());
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> tasks = new ArrayList<>();

        try (KafkaConsumer<String, Object> metadataConsumer = new KafkaConsumer<>(consumerProps)) {
            List<PartitionInfo> partitions = metadataConsumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                log.warn("No partitions found for topic {}", topic);
                return List.of();
            }

            for (PartitionInfo partition : partitions) {
                TopicPartition tp = new TopicPartition(partition.topic(), partition.partition());
                tasks.add(executor.submit(() ->
                        consumePartition(job.getJobId(), consumerProps, tp, startMillis, endMillis, allRecords)
                ));
            }

            waitForTasks(tasks, job.getJobId());
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                throw new ExtractionException("Kafka extractor did not finish within timeout", job.getJobId());
            }

            return allRecords;
        } catch (EtlException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ExtractionException("Kafka extraction interrupted", job.getJobId(), null, EtlErrorSeverity.CRITICAL, e);
        } catch (Exception e) {
            log.error("Kafka extraction failed", e);
            throw new ExtractionException("Kafka extraction failed", job.getJobId(), null, EtlErrorSeverity.CRITICAL, e);
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
            List<EtlRecord> target
    ) {
        try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.assign(List.of(tp));
            Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(Map.of(tp, startMillis));
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
            if (offsetAndTimestamp == null) {
                return;
            }

            consumer.seek(tp, offsetAndTimestamp.offset());

            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<String, Object> record : records.records(tp)) {
                    if (record.timestamp() > endMillis) {
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
                    etlRecord.put("value", record.value());

                    target.add(etlRecord);
                }
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

    private static long requireTimestamp(EtlJob job, String key) {
        Object value = job.getParam(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new ExtractionException("Parameter '%s' must be a number".formatted(key), job.getJobId());
    }
}

