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

        if (isAvro) {
            props.put("schema.registry.url", schemaRegistryUrl);
        }

        return new KafkaProducer<>(props);
    }
}
