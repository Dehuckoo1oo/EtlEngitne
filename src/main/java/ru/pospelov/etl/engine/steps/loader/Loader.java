package ru.pospelov.etl.engine.steps.loader;

import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlJob;

/**
 * Loader interface for ETL pipeline.
 *
 * <p>Loaders are responsible for writing data to target systems (SQL, Kafka, etc.)
 * and performing type conversion using TypeConverter when needed.
 *
 * <p>Loaders receive EtlBatch containing:
 * <ul>
 * <li>Collection of EtlRecord with data</li>
 * <li>Optional ColumnMetadata (from JDBC extractors) for type-aware conversion</li>
 * </ul>
 */
public interface Loader {
    /**
     * Load a batch of records to the target system.
     *
     * @param batch batch containing records and optional metadata
     * @param job ETL job configuration
     */
    void load(EtlBatch batch, EtlJob job);

    /**
     * Get loader type identifier.
     *
     * @return loader type
     */
    String getType();
}
