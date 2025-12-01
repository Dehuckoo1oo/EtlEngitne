package ru.pospelov.etl.engine.validation;

/**
 * Exception thrown when job validation fails.
 * Contains detailed information about validation errors.
 */
public class ValidationException extends RuntimeException {
    
    private final String jobId;
    private final ValidationResult validationResult;
    
    public ValidationException(String message, String jobId) {
        super(message);
        this.jobId = jobId;
        this.validationResult = null;
    }
    
    public ValidationException(String message, String jobId, ValidationResult validationResult) {
        super(message);
        this.jobId = jobId;
        this.validationResult = validationResult;
    }
    
    public ValidationException(String message, String jobId, Throwable cause) {
        super(message, cause);
        this.jobId = jobId;
        this.validationResult = null;
    }
    
    public String getJobId() {
        return jobId;
    }
    
    public ValidationResult getValidationResult() {
        return validationResult;
    }
}

