package ru.pospelov.etl.engine.steps.transformer;

import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.Record;

import java.util.Collection;

public interface Transformer {
    Collection<Record> transform(Collection<Record> records, EtlJob job);
    String getType();
}
