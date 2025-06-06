package ru.pospelov.etl.engine.steps.extractor;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class KafkaConsumerFactory {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    public Map<String, Object> buildConsumerConfig(String clientIdPrefix) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, clientIdPrefix + "-group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-" + UUID.randomUUID());
        return props;
    }
}

