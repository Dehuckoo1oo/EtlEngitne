package ru.pospelov.etl.engine.api.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.pospelov.etl.engine.api.dto.ComponentSchemaResponse;
import ru.pospelov.etl.engine.api.service.ComponentSchemaGenerator;

/**
 * REST API for getting ETL component configuration schemas.
 *
 * The schema is automatically generated from Record classes using reflection,
 * reading field types and Bean Validation annotations. This allows the frontend
 * to dynamically generate configuration forms without hardcoding field definitions.
 */
@Slf4j
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobSchemaController {

    private final ComponentSchemaGenerator schemaGenerator;

    /**
     * Get complete schema for all ETL components.
     *
     * Returns schema information for:
     * - Extractors (sql, kafka)
     * - Transformers (noop, avro, record-to-avro)
     * - Loaders (jdbc, fast-sql, kafka)
     *
     * Each schema includes:
     * - Field names and types
     * - Validation rules (required, min, max)
     * - Human-readable labels
     * - Enum values (for enum fields)
     * - Descriptions
     *
     * @return Complete component schema
     */
    @GetMapping("/schema")
    public ResponseEntity<ComponentSchemaResponse> getComponentSchema() {
        log.debug("Generating component schema");
        ComponentSchemaResponse schema = schemaGenerator.generate();
        log.debug("Generated schema with {} extractors, {} transformers, {} loaders",
                schema.getExtractors().size(),
                schema.getTransformers().size(),
                schema.getLoaders().size());
        return ResponseEntity.ok(schema);
    }
}
