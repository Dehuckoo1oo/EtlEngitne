package ru.pospelov.etl.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO для ответа на запрос запуска job'а.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobRunResponse {

    /**
     * Идентификатор запущенного job'а
     */
    @JsonProperty("jobId")
    private String jobId;

    /**
     * Статус запуска
     */
    @JsonProperty("status")
    private String status;

    /**
     * Сообщение о результате запуска
     */
    @JsonProperty("message")
    private String message;

    /**
     * Время начала выполнения (Unix timestamp в миллисекундах)
     */
    @JsonProperty("startedAt")
    private Long startedAt;
}
