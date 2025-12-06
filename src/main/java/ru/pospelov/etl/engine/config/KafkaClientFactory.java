package ru.pospelov.etl.engine.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@RequiredArgsConstructor
public class KafkaClientFactory {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    public Map<String, Object> buildConsumerConfig(String clientIdPrefix, boolean isAvro) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, clientIdPrefix + "-group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                isAvro ? KafkaAvroDeserializer.class.getName() : StringDeserializer.class.getName());

        // Performance optimizations for high-throughput (server-optimized values)
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50000);          // 50k records per poll (было 10k)
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10485760);        // 10MB min fetch (было 1MB)
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 104857600);       // 100MB max fetch
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);           // Wait max 100ms
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 52428800); // 50MB per partition (было 10MB)
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 131072);           // 128KB receive buffer

        if (isAvro) {
            props.put("schema.registry.url", schemaRegistryUrl);
            props.put("specific.avro.reader", false);
        }

        return props;
    }

    public <K, V> Consumer<K, V> createConsumer(String clientIdPrefix, boolean isAvro) {
        return new KafkaConsumer<>(buildConsumerConfig(clientIdPrefix, isAvro));
    }

    public <K, V> Producer<K, V> createProducer(boolean isAvro) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                isAvro ? KafkaAvroSerializer.class.getName() : StringSerializer.class.getName());

        // Performance optimizations for high-throughput (server-optimized values)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 262144);        // 256KB batch (было 64KB) - больше записей в батч
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);             // 20ms (было 10ms) - больше времени на накопление
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");   // Fast compression
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 268435456L); // 256MB buffer (было 64MB)
        props.put(ProducerConfig.ACKS_CONFIG, "1");                 // Leader ack only for speed
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // Параллельные запросы
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, 131072);       // 128KB send buffer

        if (isAvro) {
            props.put("schema.registry.url", schemaRegistryUrl);
        }

        return new KafkaProducer<>(props);
    }
}
