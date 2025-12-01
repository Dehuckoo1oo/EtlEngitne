package ru.pospelov.etl.engine.validation;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlJob;

import java.util.Objects;

/**
 * Validates loader-specific parameters.
 */
@Component
public class LoaderValidator implements ComponentValidator {
    
    @Override
    public void validate(EtlJob job, String componentType, ValidationResult result) {
        switch (componentType) {
            case "sql" -> validateSqlLoader(job, result);
            case "fast-sql" -> validateSqlLoader(job, result); // Uses same validation as sql
            case "kafka" -> validateKafkaLoader(job, result);
            default -> result.addError("loaderType", 
                "Unknown loader type: " + componentType + ". Available types: sql, fast-sql, kafka");
        }
    }
    
    private void validateSqlLoader(EtlJob job, ValidationResult result) {
        String targetTable = job.getTargetTable();
        if (targetTable == null || targetTable.isBlank()) {
            result.addError("targetTable", 
                "SQL loader requires 'targetTable' parameter");
        }
        
        // Validate optional parameters
        validateIntParam(job, "streamBatchSize", result, 1, Integer.MAX_VALUE);
        validateIntParam(job, "threads", result, 1, Integer.MAX_VALUE);
    }
    
    private void validateKafkaLoader(EtlJob job, ValidationResult result) {
        String topic = Objects.toString(job.getParam("topic"), "");
        if (topic.isBlank()) {
            result.addError("topic", "Kafka loader requires 'topic' parameter");
        }
        
        // Validate optional format parameter
        String format = Objects.toString(job.getParamOrDefault("format", "string"), "string");
        if (!format.equalsIgnoreCase("string") && !format.equalsIgnoreCase("avro")) {
            result.addError("format", 
                "Parameter 'format' must be 'string' or 'avro'");
        }
    }
    
    private void validateIntParam(EtlJob job, String paramName, ValidationResult result, 
                                   int minValue, int maxValue) {
        Object value = job.getParam(paramName);
        if (value != null) {
            try {
                int intValue = ((Number) value).intValue();
                if (intValue < minValue || intValue > maxValue) {
                    result.addError(paramName, 
                        String.format("Parameter '%s' must be between %d and %d", 
                            paramName, minValue, maxValue));
                }
            } catch (ClassCastException e) {
                result.addError(paramName, 
                    String.format("Parameter '%s' must be a number", paramName));
            }
        }
    }
}

