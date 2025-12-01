package ru.pospelov.etl.engine.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Result of job validation containing all validation errors.
 */
public class ValidationResult {
    
    private final List<ValidationError> errors;
    
    public ValidationResult() {
        this.errors = new ArrayList<>();
    }
    
    public void addError(String field, String message) {
        errors.add(new ValidationError(field, message));
    }
    
    public void addError(ValidationError error) {
        errors.add(error);
    }
    
    public List<ValidationError> getErrors() {
        return Collections.unmodifiableList(errors);
    }
    
    public boolean hasErrors() {
        return !errors.isEmpty();
    }
    
    public String getErrorMessage() {
        if (errors.isEmpty()) {
            return "No validation errors";
        }
        StringBuilder sb = new StringBuilder("Validation failed with ").append(errors.size()).append(" error(s):\n");
        for (int i = 0; i < errors.size(); i++) {
            ValidationError error = errors.get(i);
            sb.append("  ").append(i + 1).append(". ").append(error.getField())
              .append(": ").append(error.getMessage()).append("\n");
        }
        return sb.toString();
    }
    
    public static class ValidationError {
        private final String field;
        private final String message;
        
        public ValidationError(String field, String message) {
            this.field = field;
            this.message = message;
        }
        
        public String getField() {
            return field;
        }
        
        public String getMessage() {
            return message;
        }
    }
}

