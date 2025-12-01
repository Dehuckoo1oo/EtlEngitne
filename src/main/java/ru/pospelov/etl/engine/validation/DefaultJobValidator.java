package ru.pospelov.etl.engine.validation;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.engine.EtlComponentRegistry;
import ru.pospelov.etl.engine.model.EtlJob;

import java.util.Objects;

/**
 * Default implementation of JobValidator that validates job parameters,
 * component availability, and component-specific requirements.
 */
@Component
@RequiredArgsConstructor
public class DefaultJobValidator implements JobValidator {
    
    private final EtlComponentRegistry componentRegistry;
    private final ExtractorValidator extractorValidator;
    private final TransformerValidator transformerValidator;
    private final LoaderValidator loaderValidator;
    
    @Override
    public ValidationResult validate(EtlJob job) {
        ValidationResult result = new ValidationResult();
        
        // Validate jobId
        if (job.getJobId() == null || job.getJobId().isBlank()) {
            result.addError("jobId", "Job ID is required");
        }
        
        // Validate required component type parameters
        String extractorType = validateComponentType(job, "extractorType", result);
        String transformerType = validateComponentType(job, "transformerType", result);
        String loaderType = validateComponentType(job, "loaderType", result);
        
        // Validate component availability
        if (extractorType != null) {
            validateComponentAvailability("extractor", extractorType, 
                componentRegistry.getExtractor(extractorType) != null, result);
        }
        if (transformerType != null) {
            validateComponentAvailability("transformer", transformerType, 
                componentRegistry.getTransformer(transformerType) != null, result);
        }
        if (loaderType != null) {
            validateComponentAvailability("loader", loaderType, 
                componentRegistry.getLoader(loaderType) != null, result);
        }
        
        // Validate component-specific parameters
        if (extractorType != null) {
            extractorValidator.validate(job, extractorType, result);
        }
        if (transformerType != null) {
            transformerValidator.validate(job, transformerType, result);
        }
        if (loaderType != null) {
            loaderValidator.validate(job, loaderType, result);
        }
        
        return result;
    }
    
    private String validateComponentType(EtlJob job, String paramName, ValidationResult result) {
        Object value = job.getParam(paramName);
        if (value == null) {
            result.addError(paramName, 
                String.format("Required parameter '%s' is missing", paramName));
            return null;
        }
        String type = Objects.toString(value, "").trim();
        if (type.isEmpty()) {
            result.addError(paramName, 
                String.format("Parameter '%s' cannot be empty", paramName));
            return null;
        }
        return type;
    }
    
    private void validateComponentAvailability(String componentCategory, String componentType, 
                                                 boolean isAvailable, ValidationResult result) {
        if (!isAvailable) {
            String availableComponents = getAvailableComponents(componentCategory);
            result.addError(componentCategory + "Type", 
                String.format("%s '%s' is not registered. Available %ss: %s", 
                    capitalize(componentCategory), componentType, componentCategory, 
                    availableComponents));
        }
    }
    
    private String getAvailableComponents(String category) {
        return switch (category) {
            case "extractor" -> componentRegistry.getExtractorMap().keySet().toString();
            case "transformer" -> componentRegistry.getTransformerMap().keySet().toString();
            case "loader" -> componentRegistry.getLoaderMap().keySet().toString();
            default -> "unknown";
        };
    }
    
    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}

