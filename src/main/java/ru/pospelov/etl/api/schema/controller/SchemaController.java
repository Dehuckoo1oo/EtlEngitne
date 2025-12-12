package ru.pospelov.etl.api.schema.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.pospelov.etl.engine.schema.SchemaRegistryService;

import java.util.List;

/**
 * REST API для работы со схемами из Schema Registry.
 */
@RestController
@RequestMapping("/api/schemas")
@RequiredArgsConstructor
public class SchemaController {

    private final SchemaRegistryService schemaRegistryService;

    /**
     * Получить список всех subject'ов из Schema Registry
     *
     * @return список subject'ов
     */
    @GetMapping
    public ResponseEntity<List<String>> getAllSubjects() {
        List<String> subjects = schemaRegistryService.getAllSubjects();
        return ResponseEntity.ok(subjects);
    }

    /**
     * Проверить доступность Schema Registry
     *
     * @return статус доступности
     */
    @GetMapping("/health")
    public ResponseEntity<Boolean> checkHealth() {
        boolean isAvailable = schemaRegistryService.isAvailable();
        return ResponseEntity.ok(isAvailable);
    }
}
