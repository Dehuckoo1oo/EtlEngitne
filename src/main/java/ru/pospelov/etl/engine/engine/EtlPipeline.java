package ru.pospelov.etl.engine.engine;

import ru.pospelov.etl.engine.model.EtlJob;

public interface EtlPipeline {
    void run(EtlJob job);
}
