package ru.pospelov.etl.engine.validation;

/**
 * Ошибка валидации, используемая в {@link ValidationResult}.
 * Выделена в отдельный класс, чтобы явно показывать связь с результатом валидации.
 */
public class ValidationResultError {
    private final String field;
    private final String message;

    public ValidationResultError(String field, String message) {
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
