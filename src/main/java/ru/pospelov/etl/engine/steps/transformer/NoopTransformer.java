package ru.pospelov.etl.engine.steps.transformer;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.Record;

import java.util.Collection;

@Component
public class NoopTransformer implements Transformer {

    @Override
    public Collection<Record> transform(Collection<Record> records, EtlJob job) {
        return records;
    }

    @Override
    public String getType() {
        return "noop";
    }
}
