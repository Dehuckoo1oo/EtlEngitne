package ru.pospelov.etl.engine.steps.transformer;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlJob;

/**
 * No-operation transformer that returns batches unchanged.
 *
 * <p>This transformer is used when no transformation is needed,
 * passing data directly from extractor to loader while preserving
 * all records and metadata.
 */
@Component
public class NoopTransformer implements Transformer {

    @Override
    public EtlBatch transform(EtlBatch batch, EtlJob job) {
        // Pass through unchanged - preserve both records and metadata
        return batch;
    }

    @Override
    public String getType() {
        return "noop";
    }
}
