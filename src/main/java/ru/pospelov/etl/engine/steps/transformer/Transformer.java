package ru.pospelov.etl.engine.steps.transformer;

import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;

public interface Transformer {
    Collection<EtlRecord> transform(Collection<EtlRecord> etlRecords, EtlJob job);
    String getType();
}
