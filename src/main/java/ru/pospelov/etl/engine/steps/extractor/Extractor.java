package ru.pospelov.etl.engine.steps.extractor;

import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.Collection;

public interface Extractor {
    Collection<EtlRecord> extract(EtlJob job);
    String getType();
}