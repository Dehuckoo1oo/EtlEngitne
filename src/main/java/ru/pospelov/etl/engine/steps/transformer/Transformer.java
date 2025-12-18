package ru.pospelov.etl.engine.steps.transformer;

import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlJob;

/**
 * Transformer interface for ETL pipeline.
 *
 * <p>Transformers perform business logic transformations on data batches.
 * They do NOT perform type conversion - that is handled by Loader implementations
 * using TypeConverter.
 *
 * <p>Transformers receive EtlBatch with metadata and return transformed EtlBatch.
 * The metadata should typically be preserved unless the transformer fundamentally
 * changes the structure of records.
 */
public interface Transformer {
    /**
     * Transform a batch of records.
     *
     * @param batch input batch with records and optional metadata
     * @param job ETL job configuration
     * @return transformed batch
     */
    EtlBatch transform(EtlBatch batch, EtlJob job);

    /**
     * Get transformer type identifier.
     *
     * @return transformer type
     */
    String getType();
}
