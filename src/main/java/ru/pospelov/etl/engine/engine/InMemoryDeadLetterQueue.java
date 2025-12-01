package ru.pospelov.etl.engine.engine;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlException;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class InMemoryDeadLetterQueue implements DeadLetterQueue {

    private final CopyOnWriteArrayList<DeadLetterEntry> entries = new CopyOnWriteArrayList<>();

    @Override
    public void publish(EtlException exception) {
        Objects.requireNonNull(exception, "exception");
        entries.add(DeadLetterEntry.fromException(exception));
    }

    @Override
    public List<DeadLetterEntry> getEntries() {
        return List.copyOf(entries);
    }

    @Override
    public List<DeadLetterEntry> getEntries(String jobId) {
        return entries.stream()
                .filter(entry -> Objects.equals(entry.jobId(), jobId))
                .toList();
    }

    @Override
    public void clear() {
        entries.clear();
    }
}

