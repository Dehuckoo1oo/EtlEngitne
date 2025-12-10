package ru.pospelov.etl.engine.config.extractor;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.pospelov.etl.engine.config.KafkaFormat;

/**
 * Kafka extractor configuration.
 */
public record KafkaExtractorConfig(
    @NotBlank(message = "Kafka topic is required")
    String topic,

    @NotNull(message = "Start timestamp is required")
    Long startTimestamp,

    @NotNull(message = "End timestamp is required")
    Long endTimestamp,

    @NotNull(message = "Format is required")
    KafkaFormat format,

    @NotNull(message = "Threads is required")
    @Min(value = 1, message = "Threads must be >= 1")
    Integer threads,

    @NotNull(message = "Stream batch size is required")
    @Min(value = 100, message = "Stream batch size must be >= 100")
    @Max(value = 1_048_576, message = "Stream batch size must be <= 1,048,576 (2^20)")
    Integer streamBatchSize
) implements ExtractorConfig {

    @Override
    public String type() {
        return "kafka";
    }
}
