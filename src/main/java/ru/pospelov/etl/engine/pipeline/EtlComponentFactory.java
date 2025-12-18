package ru.pospelov.etl.engine.pipeline;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.extractor.*;
import ru.pospelov.etl.engine.config.transformer.*;
import ru.pospelov.etl.engine.config.loader.*;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.steps.extractor.jdbc.JdbcExtractor;
import ru.pospelov.etl.engine.steps.extractor.kafka.KafkaPartitionExtractor;
import ru.pospelov.etl.engine.steps.transformer.*;
import ru.pospelov.etl.engine.steps.loader.*;

import java.util.function.Consumer;

/**
 * Centralized factory for creating and running ETL components.
 * Uses exhaustive pattern matching for type-safe dispatching.
 *
 * <p>All methods now use EtlBatch instead of Collection<EtlRecord>
 * to support batch-level metadata (e.g., JDBC column metadata).
 */
@Component
@RequiredArgsConstructor
public class EtlComponentFactory {

    private final JdbcExtractor jdbcExtractor;
    private final KafkaPartitionExtractor kafkaPartitionExtractor;

    private final NoopTransformer noopTransformer;
    private final AvroToRecordTransformer avroToRecordTransformer;
    private final RecordToAvroTransformer recordToAvroTransformer;

    private final JdbcLoader jdbcLoader;
    private final FastSqlServerLoader fastSqlLoader;
    private final KafkaByPartitionLoader kafkaByPartitionLoader;

    /**
     * Extract data using extractor configuration.
     * Pattern matching guarantees handling of all types.
     *
     * @param job the ETL job
     * @param batchConsumer consumer that receives extracted batches
     */
    public void extract(EtlJob job, Consumer<EtlBatch> batchConsumer) {
        switch (job.extractorConfig()) {
            case JdbcExtractorConfig config ->
                jdbcExtractor.extract(config, job.jobId(), batchConsumer);

            case KafkaExtractorConfig config ->
                    kafkaPartitionExtractor.extract(config, job.jobId(), batchConsumer);

            // Compiler checks exhaustiveness!
            // If a new sealed type is added - code won't compile without handling it
        }
    }

    /**
     * Transform data using transformer configuration.
     *
     * @param job the ETL job
     * @param batch input batch
     * @return transformed batch
     */
    public EtlBatch transform(EtlJob job, EtlBatch batch) {
        return switch (job.transformerConfig()) {
            case NoopTransformerConfig config ->
                noopTransformer.transform(batch, job);

            case AvroToRecordTransformerConfig config ->
                avroToRecordTransformer.transform(batch);

            case RecordToAvroTransformerConfig config ->
                recordToAvroTransformer.transform(batch, config);
        };
    }

    /**
     * Load data using loader configuration.
     *
     * @param job the ETL job
     * @param batch batch to load
     */
    public void load(EtlJob job, EtlBatch batch) {
        switch (job.loaderConfig()) {
            case JdbcLoaderConfig config ->
                jdbcLoader.load(config, job.jobId(), batch);

            case FastSqlLoaderConfig config ->
                fastSqlLoader.load(config, job.jobId(), batch);

            case KafkaLoaderConfig config ->
                kafkaByPartitionLoader.load(config, job.jobId(), batch);
        }
    }
}
