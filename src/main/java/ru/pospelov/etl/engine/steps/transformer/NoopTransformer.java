package ru.pospelov.etl.engine.steps.transformer;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;

@Component
public class NoopTransformer {

    /**
     * No-operation transformer - returns records unchanged.
     */
    public Collection<EtlRecord> transform(Collection<EtlRecord> etlRecords) {
        return etlRecords;
    }
}
