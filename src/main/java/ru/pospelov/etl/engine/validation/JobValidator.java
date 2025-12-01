package ru.pospelov.etl.engine.validation;

import ru.pospelov.etl.engine.model.EtlJob;

/**
 * Interface for validating ETL job parameters and configuration.
 */
public interface JobValidator {
    
    /**
     * Validates the given ETL job.
     * 
     * @param job the job to validate
     * @return validation result containing any errors
     */
    ValidationResult validate(EtlJob job);
    
    /**
     * Validates the given ETL job and throws ValidationException if validation fails.
     * 
     * @param job the job to validate
     * @throws ValidationException if validation fails
     */
    default void validateOrThrow(EtlJob job) {
        ValidationResult result = validate(job);
        if (result.hasErrors()) {
            throw new ValidationException(
                "Job validation failed for jobId: " + job.getJobId(),
                job.getJobId(),
                result
            );
        }
    }
}

