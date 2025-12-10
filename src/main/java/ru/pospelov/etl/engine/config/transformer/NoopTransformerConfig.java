package ru.pospelov.etl.engine.config.transformer;

public record NoopTransformerConfig() implements TransformerConfig {
    @Override
    public String type() {
        return "noop";
    }
}
