package ru.pospelov.etl.engine.config.loader;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.pospelov.etl.engine.config.KafkaFormat;

public record KafkaLoaderConfig(
    @NotBlank(message = "Kafka topic is required")
    String topic,

    @NotNull(message = "Format is required")
    KafkaFormat format
) implements LoaderConfig {
    @Override
    public String type() {
        return "kafka";
    }
}
