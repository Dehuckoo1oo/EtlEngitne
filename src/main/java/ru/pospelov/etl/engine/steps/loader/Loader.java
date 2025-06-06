package ru.pospelov.etl.engine.steps.loader;

import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.Record;

import java.util.Collection;

public interface Loader {
    void load(Collection<Record> records, EtlJob job);
    String getType();
}
