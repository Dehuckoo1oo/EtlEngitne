package ru.pospelov.etl.engine.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Result of job validation containing all validation errors.
 */
public class ValidationResult {
    
    private final List<ValidationResultError> errors;
    
    public ValidationResult() {
        this.errors = new ArrayList<>();
    }
    
    public void addError(String field, String message) {
        errors.add(new ValidationResultError(field, message));
    }
    
    public void addError(ValidationResultError error) {
        errors.add(error);
    }
    
    public List<ValidationResultError> getErrors() {
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
            ValidationResultError error = errors.get(i);
            sb.append("  ").append(i + 1).append(". ").append(error.getField())
              .append(": ").append(error.getMessage()).append("\n");
        }
        return sb.toString();
    }
}
