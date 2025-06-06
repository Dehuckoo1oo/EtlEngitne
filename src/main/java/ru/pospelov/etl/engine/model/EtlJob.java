package ru.pospelov.etl.engine.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;

@AllArgsConstructor
@Getter
@ToString
public class EtlJob {
    private final String jobId;
    private final String sourceQuery;
    private final String targetTable;
    private final Map<String, Object> parameters;

    public Object getParam(String key) {
        return parameters.get(key);
    }

    public Object getParamOrDefault(String key, Object defaultValue) {
        return parameters.getOrDefault(key, defaultValue);
    }
}