package ru.pospelov.etl.engine.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * DTO для входящих запросов создания/обновления ETL job'а.
 * Используется для десериализации JSON из REST API.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobRequest {

    /**
     * Уникальный идентификатор job'а
     */
    @JsonProperty("id")
    private String id;

    /**
     * Источник данных:
     * - SQL-запрос для извлечения данных (для SQL extractor)
     * - Kafka топик (для Kafka extractor)
     * Может быть null в зависимости от типа extractor
     */
    @JsonProperty("source")
    private String source;

    /**
     * Целевая таблица для загрузки данных (для SQL loader)
     * Может быть null для Kafka loader
     */
    @JsonProperty("target")
    private String target;

    /**
     * Параметры конфигурации job'а:
     * - extractorType: тип экстрактора (sql, kafka)
     * - transformerType: тип трансформера (noop, avro)
     * - loaderType: тип загрузчика (jdbc, kafka, fast-sql)
     * - topic: топик Kafka (для Kafka extractor/loader)
     * - format: формат данных (avro, json)
     * - threads: количество потоков для параллельной обработки
     * - streamBatchSize: размер батча для потоковой обработки
     * - avroSchema: схема Avro для сериализации/десериализации
     * - keyColumn: колонка для ключа Kafka сообщения
     * - partitionColumn: колонка для партиционирования
     * - partitions: количество партиций
     * - startTimestamp: начальная временная метка для Kafka (мс)
     * - endTimestamp: конечная временная метка для Kafka (мс)
     * - и другие специфичные параметры
     */
    @JsonProperty("params")
    private Map<String, Object> params = new HashMap<>();
}
