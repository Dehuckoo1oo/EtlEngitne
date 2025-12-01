package ru.pospelov.etl.engine.validation;

import ru.pospelov.etl.engine.model.EtlJob;

/**
 * Interface for validating component-specific parameters.
 */
public interface ComponentValidator {
    
    /**
     * Validates parameters for a specific component type.
     * 
     * @param job the job containing parameters
     * @param componentType the type of component being validated
     * @param result the validation result to add errors to
     */
    void validate(EtlJob job, String componentType, ValidationResult result);
}

