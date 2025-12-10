package ru.pospelov.etl.engine.engine;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.extractor.*;
import ru.pospelov.etl.engine.config.transformer.*;
import ru.pospelov.etl.engine.config.loader.*;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.steps.extractor.jdbc.JdbcExtractor;
import ru.pospelov.etl.engine.steps.extractor.kafka.KafkaPartitionExtractor;
import ru.pospelov.etl.engine.steps.transformer.*;
import ru.pospelov.etl.engine.steps.loader.*;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Centralized factory for creating and running ETL components.
 * Uses exhaustive pattern matching for type-safe dispatching.
 */
@Component
@RequiredArgsConstructor
public class EtlComponentFactory {

    private final JdbcExtractor jdbcExtractor;
    private final KafkaPartitionExtractor kafkaExtractor;

    private final NoopTransformer noopTransformer;
    private final AvroToRecordTransformer avroToRecordTransformer;
    private final RecordToAvroTransformer recordToAvroTransformer;

    private final JdbcLoader jdbcLoader;
    private final FastSqlServerLoader fastSqlLoader;
    private final KafkaLoader kafkaLoader;

    /**
     * Extract data using extractor configuration.
     * Pattern matching guarantees handling of all types.
     */
    public void extract(EtlJob job, Consumer<Collection<EtlRecord>> batchConsumer) {
        switch (job.extractorConfig()) {
            case JdbcExtractorConfig config ->
                jdbcExtractor.extract(config, job.jobId(), batchConsumer);

            case KafkaExtractorConfig config ->
                kafkaExtractor.extract(config, job.jobId(), batchConsumer);

            // Compiler checks exhaustiveness!
            // If a new sealed type is added - code won't compile without handling it
        }
    }

    /**
     * Transform data using transformer configuration.
     */
    public Collection<EtlRecord> transform(EtlJob job, Collection<EtlRecord> records) {
        return switch (job.transformerConfig()) {
            case NoopTransformerConfig config ->
                noopTransformer.transform(records);

            case AvroToRecordTransformerConfig config ->
                avroToRecordTransformer.transform(records);

            case RecordToAvroTransformerConfig config ->
                recordToAvroTransformer.transform(records, config);
        };
    }

    /**
     * Load data using loader configuration.
     */
    public void load(EtlJob job, Collection<EtlRecord> records) {
        switch (job.loaderConfig()) {
            case JdbcLoaderConfig config ->
                jdbcLoader.load(config, job.jobId(), records);

            case FastSqlLoaderConfig config ->
                fastSqlLoader.load(config, job.jobId(), records);

            case KafkaLoaderConfig config ->
                kafkaLoader.load(config, job.jobId(), records);
        }
    }
}
