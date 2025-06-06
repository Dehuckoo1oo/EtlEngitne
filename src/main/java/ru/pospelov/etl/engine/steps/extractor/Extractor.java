package ru.pospelov.etl.engine.steps.extractor;

import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.Record;

import java.util.Collection;

public interface Extractor {
    Collection<Record> extract(EtlJob job);
    String getType();
}