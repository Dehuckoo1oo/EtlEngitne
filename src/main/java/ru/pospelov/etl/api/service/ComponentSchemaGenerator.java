package ru.pospelov.etl.api.service;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.stereotype.Service;
import ru.pospelov.etl.api.dto.ComponentSchema;
import ru.pospelov.etl.api.dto.ComponentSchemaResponse;
import ru.pospelov.etl.api.dto.FieldSchema;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.config.extractor.KafkaExtractorConfig;
import ru.pospelov.etl.engine.config.loader.FastSqlLoaderConfig;
import ru.pospelov.etl.engine.config.loader.JdbcLoaderConfig;
import ru.pospelov.etl.engine.config.loader.KafkaLoaderConfig;
import ru.pospelov.etl.engine.config.transformer.AvroToRecordTransformerConfig;
import ru.pospelov.etl.engine.config.transformer.NoopTransformerConfig;
import ru.pospelov.etl.engine.config.transformer.RecordToAvroTransformerConfig;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Schema generator from Record classes using reflection.
 * Reads Record fields and Bean Validation annotations to auto-generate
 * configuration schema for the frontend.
 */
@Service
public class ComponentSchemaGenerator {

    /**
     * Generate complete schema for all ETL components
     */
    public ComponentSchemaResponse generate() {
        return ComponentSchemaResponse.builder()
            .extractors(Map.of(
                "sql", generateSchema(JdbcExtractorConfig.class, "SQL Database"),
                "kafka", generateSchema(KafkaExtractorConfig.class, "Kafka Topic")
            ))
            .transformers(Map.of(
                "noop", generateSchema(NoopTransformerConfig.class, "No Operation"),
                "avro", generateSchema(AvroToRecordTransformerConfig.class, "Avro to Record"),
                "record-to-avro", generateSchema(RecordToAvroTransformerConfig.class, "Record to Avro")
            ))
            .loaders(Map.of(
                "jdbc", generateSchema(JdbcLoaderConfig.class, "JDBC Loader"),
                "fast-sql", generateSchema(FastSqlLoaderConfig.class, "Fast SQL (Bulk Insert)"),
                "kafka", generateSchema(KafkaLoaderConfig.class, "Kafka Topic")
            ))
            .build();
    }

    /**
     * Generate schema for a single Record class
     */
    private ComponentSchema generateSchema(Class<?> recordClass, String displayName) {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class must be a Record: " + recordClass.getName());
        }

        RecordComponent[] components = recordClass.getRecordComponents();
        List<FieldSchema> fields = new ArrayList<>();

        for (RecordComponent component : components) {
            // Skip "type" field as it's automatically determined
            if ("type".equals(component.getName())) {
                continue;
            }

            fields.add(generateFieldSchema(component));
        }

        return ComponentSchema.builder()
            .displayName(displayName)
            .fields(fields)
            .build();
    }

    /**
     * Generate schema for a single field
     */
    private FieldSchema generateFieldSchema(RecordComponent component) {
        Class<?> fieldType = component.getType();
        Type genericType = component.getGenericType();

        FieldSchema.FieldSchemaBuilder builder = FieldSchema.builder()
            .name(component.getName())
            .label(formatLabel(component.getName()))
            .required(isRequired(component));

        // Determine type
        if (fieldType == Optional.class) {
            // Extract inner type from Optional<T>
            if (genericType instanceof ParameterizedType paramType) {
                Type[] typeArgs = paramType.getActualTypeArguments();
                if (typeArgs.length > 0 && typeArgs[0] instanceof Class<?> innerClass) {
                    builder.type(mapType(innerClass));
                    builder.required(false); // Optional fields are never required

                    // Handle enum inside Optional
                    if (innerClass.isEnum()) {
                        builder.enumValues(getEnumValues(innerClass));
                    }
                } else {
                    builder.type("text");
                }
            } else {
                builder.type("text");
            }
        } else {
            builder.type(mapType(fieldType));

            // Handle enum
            if (fieldType.isEnum()) {
                builder.enumValues(getEnumValues(fieldType));
            }
        }

        // Add validation constraints
        Integer min = getMin(component);
        Integer max = getMax(component);

        if (min != null) {
            builder.min(min);
        }
        if (max != null) {
            builder.max(max);
        }

        // Add descriptions based on field name
        builder.description(generateDescription(component.getName(), fieldType));

        return builder.build();
    }

    /**
     * Map Java type to frontend type
     */
    private String mapType(Class<?> javaType) {
        if (javaType == String.class) return "text";
        if (javaType == Integer.class || javaType == int.class) return "number";
        if (javaType == Long.class || javaType == long.class) return "number";
        if (javaType == Boolean.class || javaType == boolean.class) return "boolean";
        if (javaType.isEnum()) return "enum";
        return "text";
    }

    /**
     * Check if field is required based on validation annotations
     */
    private boolean isRequired(RecordComponent component) {
        return component.isAnnotationPresent(NotNull.class)
            || component.isAnnotationPresent(NotBlank.class);
    }

    /**
     * Extract @Min value
     */
    private Integer getMin(RecordComponent component) {
        Min min = component.getAnnotation(Min.class);
        return min != null ? (int) min.value() : null;
    }

    /**
     * Extract @Max value
     */
    private Integer getMax(RecordComponent component) {
        Max max = component.getAnnotation(Max.class);
        return max != null ? (int) max.value() : null;
    }

    /**
     * Get enum values as strings
     */
    private List<String> getEnumValues(Class<?> enumClass) {
        if (!enumClass.isEnum()) {
            return null;
        }
        return Arrays.stream(enumClass.getEnumConstants())
            .map(Object::toString)
            .collect(Collectors.toList());
    }

    /**
     * Format camelCase field name to readable label
     * sqlQuery -> SQL Query
     * streamBatchSize -> Stream Batch Size
     */
    private String formatLabel(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            return fieldName;
        }

        // Add space before capital letters
        String withSpaces = fieldName.replaceAll("([A-Z])", " $1").trim();

        // Capitalize first letter
        return Character.toUpperCase(withSpaces.charAt(0)) + withSpaces.substring(1);
    }

    /**
     * Generate helpful description based on field name and type
     */
    private String generateDescription(String fieldName, Class<?> fieldType) {
        return switch (fieldName) {
            case "sqlQuery" -> "SQL query to extract data from database";
            case "partitionColumn" -> "Column name for partitioning (optional)";
            case "partitions" -> "Number of parallel partitions";
            case "keyColumn" -> "Column to use as record key (optional)";
            case "threads" -> "Number of parallel threads for processing";
            case "streamBatchSize" -> "Number of records per batch";
            case "topic" -> "Kafka topic name";
            case "startTimestamp" -> "Start timestamp in milliseconds (epoch)";
            case "endTimestamp" -> "End timestamp in milliseconds (epoch)";
            case "format" -> "Data format (AVRO, JSON, STRING)";
            case "avroSchemaSubject" -> "Avro schema subject name in Schema Registry";
            case "targetTable" -> "Target table name in database";
            default -> null;
        };
    }
}
