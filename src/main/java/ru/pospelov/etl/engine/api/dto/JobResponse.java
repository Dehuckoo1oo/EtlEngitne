package ru.pospelov.etl.engine.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * DTO для ответов API с информацией о ETL job'е.
 * Используется для сериализации job'а в JSON.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobResponse {

    /**
     * Уникальный идентификатор job'а
     */
    @JsonProperty("id")
    private String id;

    /**
     * Источник данных (SQL-запрос или Kafka топик)
     */
    @JsonProperty("source")
    private String source;

    /**
     * Целевая таблица для загрузки данных
     */
    @JsonProperty("target")
    private String target;

    /**
     * Параметры конфигурации job'а
     */
    @JsonProperty("params")
    private Map<String, Object> params;
}
