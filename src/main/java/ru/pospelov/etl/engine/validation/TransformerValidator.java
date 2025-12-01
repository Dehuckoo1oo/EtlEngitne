package ru.pospelov.etl.engine.validation;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlJob;

/**
 * Validates transformer-specific parameters.
 */
@Component
public class TransformerValidator implements ComponentValidator {
    
    @Override
    public void validate(EtlJob job, String componentType, ValidationResult result) {
        switch (componentType) {
            case "noop" -> {
                // NoopTransformer doesn't require any parameters
            }
            case "record-to-avro" -> validateRecordToAvroTransformer(job, result);
            case "avro" -> {
                // AvroToRecordTransformer doesn't require parameters, but expects Avro data
            }
            default -> result.addError("transformerType", 
                "Unknown transformer type: " + componentType + 
                ". Available types: noop, record-to-avro, avro");
        }
    }
    
    private void validateRecordToAvroTransformer(EtlJob job, ValidationResult result) {
        Object avroSchema = job.getParam("avroSchema");
        if (avroSchema == null) {
            result.addError("avroSchema", 
                "Transformer 'record-to-avro' requires 'avroSchema' parameter");
        } else {
            // Validate that avroSchema is either a String or Avro Schema object
            if (!(avroSchema instanceof String) && 
                !avroSchema.getClass().getName().equals("org.apache.avro.Schema")) {
                result.addError("avroSchema", 
                    "Parameter 'avroSchema' must be a String or Avro Schema object");
            } else if (avroSchema instanceof String schemaStr) {
                // Try to parse the schema to validate it
                try {
                    new org.apache.avro.Schema.Parser().parse(schemaStr);
                } catch (Exception e) {
                    result.addError("avroSchema", 
                        "Parameter 'avroSchema' contains invalid Avro schema: " + e.getMessage());
                }
            }
        }
    }
}

