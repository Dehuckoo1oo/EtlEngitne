package ru.pospelov.etl.engine.steps.loader;

import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;

public interface Loader {
    void load(Collection<EtlRecord> etlRecords, EtlJob job);
    String getType();
}
