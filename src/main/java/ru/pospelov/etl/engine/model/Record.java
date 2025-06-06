package ru.pospelov.etl.engine.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@AllArgsConstructor
@Getter
@ToString
public class Record {
    private final Map<String, Object> fields = new ConcurrentHashMap<>();
    private final Instant timestamp;
    private final String sourcePartition;
    private final long offset;

    public void put(String key, Object value) {
        fields.put(key, value);
    }

    public Object get(String key) {
        return fields.get(key);
    }

    public Map<String, Object> getAll() {
        return Collections.unmodifiableMap(fields);
    }
}