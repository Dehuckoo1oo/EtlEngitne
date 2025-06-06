package ru.pospelov.etl.engine.steps.extractor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.Record;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaExtractor implements Extractor {

    private final KafkaConsumerFactory consumerFactory;

    @Override
    public String getType() {
        return "kafka";
    }

    @Override
    public Collection<Record> extract(EtlJob job) {
        String topic = job.getParam("topic").toString();
        long startMillis = (long) job.getParam("startTimestamp");
        long endMillis = (long) job.getParam("endTimestamp");
        int threadCount = (int) job.getParamOrDefault("threads", 4);

        Map<String, Object> consumerProps = consumerFactory.buildConsumerConfig("extractor-client");
        List<Record> allRecords = Collections.synchronizedList(new ArrayList<>());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            List<Future<?>> tasks = new ArrayList<>();

            for (PartitionInfo partition : partitions) {
                tasks.add(executor.submit(() -> {
                    TopicPartition tp = new TopicPartition(partition.topic(), partition.partition());
                    try (KafkaConsumer<String, String> partConsumer = new KafkaConsumer<>(consumerProps)) {
                        partConsumer.assign(List.of(tp));

                        Map<TopicPartition, OffsetAndTimestamp> offsets = partConsumer.offsetsForTimes(
                                Map.of(tp, startMillis)
                        );

                        OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
                        if (offsetAndTimestamp == null) return;

                        partConsumer.seek(tp, offsetAndTimestamp.offset());

                        while (true) {
                            ConsumerRecords<String, String> records = partConsumer.poll(Duration.ofMillis(500));
                            if (records.isEmpty()) break;

                            for (ConsumerRecord<String, String> r : records.records(tp)) {
                                if (r.timestamp() > endMillis) return;
                                Record rec = new Record(
                                        Instant.ofEpochMilli(r.timestamp()),
                                        tp.toString(),
                                        r.offset()
                                );
                                rec.put("key", r.key());
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
        } catch (Exception e) {
            log.error("Kafka extraction failed", e);
            throw new RuntimeException(e);
        }

        return allRecords;
    }
}
