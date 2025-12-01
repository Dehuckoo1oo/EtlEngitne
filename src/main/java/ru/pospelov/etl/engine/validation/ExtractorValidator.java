package ru.pospelov.etl.engine.validation;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlJob;

import java.util.Objects;

/**
 * Validates extractor-specific parameters.
 */
@Component
public class ExtractorValidator implements ComponentValidator {
    
    @Override
    public void validate(EtlJob job, String componentType, ValidationResult result) {
        switch (componentType) {
            case "sql" -> validateSqlExtractor(job, result);
            case "kafka" -> validateKafkaExtractor(job, result);
            default -> result.addError("extractorType", 
                "Unknown extractor type: " + componentType + ". Available types: sql, kafka");
        }
    }
    
    private void validateSqlExtractor(EtlJob job, ValidationResult result) {
        String query = job.getSourceQuery();
        if (query == null || query.isBlank()) {
            query = Objects.toString(job.getParam("query"), "");
        }
        if (query.isBlank()) {
            result.addError("sourceQuery", 
                "SQL extractor requires 'sourceQuery' or 'query' parameter");
        }
        
        // Validate optional parameters with type checking
        validateIntParam(job, "streamBatchSize", result, 1, Integer.MAX_VALUE);
        validateIntParam(job, "threads", result, 1, Integer.MAX_VALUE);
        validateIntParam(job, "partitions", result, 1, Integer.MAX_VALUE);
        
        // If partitionColumn is specified, partitions must be > 1
        String partitionColumn = Objects.toString(job.getParam("partitionColumn"), "");
        if (!partitionColumn.isEmpty()) {
            Object partitionsObj = job.getParam("partitions");
            if (partitionsObj == null) {
                result.addError("partitions", 
                    "Parameter 'partitions' is required when 'partitionColumn' is specified");
            } else {
                try {
                    int partitions = ((Number) partitionsObj).intValue();
                    if (partitions <= 1) {
                        result.addError("partitions", 
                            "Parameter 'partitions' must be greater than 1 when 'partitionColumn' is specified");
                    }
                } catch (ClassCastException e) {
                    result.addError("partitions", 
                        "Parameter 'partitions' must be a number");
                }
            }
        }
        
        // Validate avroSchema if provided
        Object avroSchema = job.getParam("avroSchema");
        if (avroSchema != null && !(avroSchema instanceof String) && 
            !avroSchema.getClass().getName().equals("org.apache.avro.Schema")) {
            result.addError("avroSchema", 
                "Parameter 'avroSchema' must be a String or Avro Schema object");
        }
    }
    
    private void validateKafkaExtractor(EtlJob job, ValidationResult result) {
        // Validate required parameters
        String topic = Objects.toString(job.getParam("topic"), "");
        if (topic.isBlank()) {
            result.addError("topic", "Kafka extractor requires 'topic' parameter");
        }
        
        // Validate timestamps
        Object startTimestamp = job.getParam("startTimestamp");
        if (startTimestamp == null) {
            result.addError("startTimestamp", 
                "Kafka extractor requires 'startTimestamp' parameter (milliseconds)");
        } else if (!(startTimestamp instanceof Number)) {
            result.addError("startTimestamp", 
                "Parameter 'startTimestamp' must be a number (milliseconds)");
        }
        
        Object endTimestamp = job.getParam("endTimestamp");
        if (endTimestamp == null) {
            result.addError("endTimestamp", 
                "Kafka extractor requires 'endTimestamp' parameter (milliseconds)");
        } else if (!(endTimestamp instanceof Number)) {
            result.addError("endTimestamp", 
                "Parameter 'endTimestamp' must be a number (milliseconds)");
        }
        
        // Validate timestamp relationship
        if (startTimestamp instanceof Number start && endTimestamp instanceof Number end) {
            if (start.longValue() >= end.longValue()) {
                result.addError("startTimestamp", 
                    "Parameter 'startTimestamp' must be before 'endTimestamp'");
            }
        }
        
        // Validate optional parameters
        validateIntParam(job, "streamBatchSize", result, 1, Integer.MAX_VALUE);
        validateIntParam(job, "threads", result, 1, Integer.MAX_VALUE);
        
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

