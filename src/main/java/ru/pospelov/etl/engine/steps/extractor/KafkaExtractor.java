package ru.pospelov.etl.engine.steps.extractor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.KafkaClientFactory;
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
        String topic = job.getParam("topic").toString();
        long startMillis = (long) job.getParam("startTimestamp");
        long endMillis = (long) job.getParam("endTimestamp");
        int threadCount = (int) job.getParamOrDefault("threads", 4);
        String format = String.valueOf(job.getParamOrDefault("format", "string")); // "string" или "avro"

        boolean isAvro = format.equalsIgnoreCase("avro");

        Map<String, Object> consumerProps = consumerFactory.buildConsumerConfig("kafka-extractor", isAvro);
        List<EtlRecord> allRecords = Collections.synchronizedList(new ArrayList<>());

        try (KafkaConsumer<String, Object> metadataConsumer = new KafkaConsumer<>(consumerProps)) {
            List<PartitionInfo> partitions = metadataConsumer.partitionsFor(topic);
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            List<Future<?>> tasks = new ArrayList<>();

            for (PartitionInfo partition : partitions) {
                tasks.add(executor.submit(() -> {
                    TopicPartition tp = new TopicPartition(partition.topic(), partition.partition());
                    try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerProps)) {
                        consumer.assign(List.of(tp));
                        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(Map.of(tp, startMillis));
                        OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
                        if (offsetAndTimestamp == null) return;

                        consumer.seek(tp, offsetAndTimestamp.offset());

                        while (true) {
                            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(500));
                            if (records.isEmpty()) break;

                            for (ConsumerRecord<String, Object> r : records.records(tp)) {
                                if (r.timestamp() > endMillis) return;
                                EtlRecord rec = new EtlRecord(
                                        Instant.ofEpochMilli(r.timestamp()),
                                        tp.toString(),
                                        r.offset()
                                );
                                if (r.key() != null) {
                                    rec.put("key", r.key());
                                }
                                rec.put("value", r.value());

                                allRecords.add(rec);
                            }
                        }
                    }
                }));
            }

            for (Future<?> task : tasks) {
                task.get();
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);

        } catch (Exception e) {
            log.error("Kafka extraction failed", e);
            throw new RuntimeException(e);
        }

        return allRecords;
    }
}

