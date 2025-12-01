package ru.pospelov.etl.engine.engine;

import ru.pospelov.etl.engine.exception.EtlException;

import java.util.List;

public interface DeadLetterQueue {
    void publish(EtlException exception);

    List<DeadLetterEntry> getEntries();

    List<DeadLetterEntry> getEntries(String jobId);

    void clear();
}

