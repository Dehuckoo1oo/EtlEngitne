package ru.pospelov.etl.api.job.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * DTO для запроса ручного запуска job'а.
 * Позволяет переопределить параметры при запуске.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobRunRequest {

    /**
     * Опциональные параметры для переопределения при запуске.
     * Если указаны, они будут объединены с существующими параметрами job'а.
     */
    @JsonProperty("params")
    private Map<String, Object> params = new HashMap<>();
}
