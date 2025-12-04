package ru.pospelov.etl.engine.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * Сервис для работы с Confluent Schema Registry.
 * Позволяет получать список subject'ов и схемы по subject name.
 */
@Slf4j
@Service
public class SchemaRegistryService {

    private final String schemaRegistryUrl;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public SchemaRegistryService(@Value("${kafka.schema-registry-url}") String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Получает список всех subject'ов из Schema Registry
     *
     * @return список названий subject'ов
     */
    public List<String> getAllSubjects() {
        try {
            String url = schemaRegistryUrl + "/subjects";
            String response = restTemplate.getForObject(url, String.class);

            JsonNode subjects = objectMapper.readTree(response);
            List<String> result = new ArrayList<>();

            if (subjects.isArray()) {
                subjects.forEach(subject -> result.add(subject.asText()));
            }

            log.debug("Found {} subjects in Schema Registry", result.size());
            return result;

        } catch (Exception e) {
            log.error("Failed to fetch subjects from Schema Registry: {}", e.getMessage());
            return List.of();
        }
    }

    /**
     * Получает последнюю версию схемы по subject name
     *
     * @param subject название subject'а
     * @return Avro схема
     * @throws RuntimeException если схема не найдена или произошла ошибка
     */
    public Schema getLatestSchema(String subject) {
        try {
            String url = schemaRegistryUrl + "/subjects/" + subject + "/versions/latest";
            String response = restTemplate.getForObject(url, String.class);

            JsonNode jsonNode = objectMapper.readTree(response);
            String schemaStr = jsonNode.get("schema").asText();

            Schema schema = new Schema.Parser().parse(schemaStr);
            log.debug("Fetched schema for subject '{}': {}", subject, schema.getName());

            return schema;

        } catch (Exception e) {
            log.error("Failed to fetch schema for subject '{}': {}", subject, e.getMessage());
            throw new RuntimeException("Failed to fetch schema for subject: " + subject, e);
        }
    }

    /**
     * Получает схему по subject name и версии
     *
     * @param subject название subject'а
     * @param version версия схемы
     * @return Avro схема
     * @throws RuntimeException если схема не найдена или произошла ошибка
     */
    public Schema getSchema(String subject, int version) {
        try {
            String url = schemaRegistryUrl + "/subjects/" + subject + "/versions/" + version;
            String response = restTemplate.getForObject(url, String.class);

            JsonNode jsonNode = objectMapper.readTree(response);
            String schemaStr = jsonNode.get("schema").asText();

            return new Schema.Parser().parse(schemaStr);

        } catch (Exception e) {
            log.error("Failed to fetch schema for subject '{}' version {}: {}", subject, version, e.getMessage());
            throw new RuntimeException("Failed to fetch schema for subject: " + subject + " version: " + version, e);
        }
    }

    /**
     * Проверяет доступность Schema Registry
     *
     * @return true если Schema Registry доступен
     */
    public boolean isAvailable() {
        try {
            restTemplate.getForObject(schemaRegistryUrl + "/subjects", String.class);
            return true;
        } catch (Exception e) {
            log.warn("Schema Registry is not available: {}", e.getMessage());
            return false;
        }
    }
}
