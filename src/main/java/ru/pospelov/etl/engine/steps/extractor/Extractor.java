package ru.pospelov.etl.engine.steps.extractor;

import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Extractor component that extracts data from a source in batches.
 * Instead of loading all data into memory, it processes data in configurable batches
 * and invokes the consumer for each batch, enabling streaming processing.
 */
public interface Extractor {
    /**
     * Extracts data from the source and processes it in batches.
     * The batchConsumer is invoked for each batch of records extracted.
     * 
     * @param job the ETL job configuration
     * @param batchConsumer consumer that receives each batch of records for processing
     */
    void extract(EtlJob job, Consumer<Collection<EtlRecord>> batchConsumer);
    
    String getType();
}