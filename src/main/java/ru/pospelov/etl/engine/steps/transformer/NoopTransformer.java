package ru.pospelov.etl.engine.steps.transformer;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;

@Component
public class NoopTransformer implements Transformer {

    @Override
    public Collection<EtlRecord> transform(Collection<EtlRecord> etlRecords, EtlJob job) {
        return etlRecords;
    }

    @Override
    public String getType() {
        return "noop";
    }
}
