package ru.pospelov.etl.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.pospelov.etl.engine.engine.EtlComponentRegistry;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.steps.extractor.Extractor;
import ru.pospelov.etl.engine.steps.loader.Loader;
import ru.pospelov.etl.engine.steps.transformer.Transformer;
import ru.pospelov.etl.engine.validation.*;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JobValidationTest {

    private EtlComponentRegistry componentRegistry;
    private ExtractorValidator extractorValidator;
    private TransformerValidator transformerValidator;
    private LoaderValidator loaderValidator;
    private DefaultJobValidator jobValidator;

    @BeforeEach
    void setUp() {
        componentRegistry = mock(EtlComponentRegistry.class);
        extractorValidator = new ExtractorValidator();
        transformerValidator = new TransformerValidator();
        loaderValidator = new LoaderValidator();
        jobValidator = new DefaultJobValidator(
                componentRegistry,
                extractorValidator,
                transformerValidator,
                loaderValidator
        );
    }

    @Test
    void validationResultShouldTrackErrors() {
        ValidationResult result = new ValidationResult();
        
        assertThat(result.hasErrors()).isFalse();
        
        result.addError("field1", "Error message 1");
        result.addError("field2", "Error message 2");
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).hasSize(2);
        assertThat(result.getErrors().get(0).getField()).isEqualTo("field1");
        assertThat(result.getErrors().get(0).getMessage()).isEqualTo("Error message 1");
        assertThat(result.getErrorMessage()).contains("2 error(s)");
    }

    @Test
    void validationExceptionShouldContainJobId() {
        ValidationException exception = new ValidationException("Test error", "job-123");
        
        assertThat(exception.getJobId()).isEqualTo("job-123");
        assertThat(exception.getMessage()).isEqualTo("Test error");
    }

    @Test
    void validateJobWithMissingJobId() {
        EtlJob job = new EtlJob(null, null, null, Map.of());
        
        ValidationResult result = jobValidator.validate(job);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> e.getField().equals("jobId"));
    }

    @Test
    void validateJobWithMissingRequiredComponentTypes() {
        EtlJob job = new EtlJob("test-job", null, null, Map.of());
        
        ValidationResult result = jobValidator.validate(job);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> e.getField().equals("extractorType"));
        assertThat(result.getErrors()).anyMatch(e -> e.getField().equals("transformerType"));
        assertThat(result.getErrors()).anyMatch(e -> e.getField().equals("loaderType"));
    }

    @Test
    void validateJobWithUnavailableComponents() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "unknown-extractor");
        params.put("transformerType", "unknown-transformer");
        params.put("loaderType", "unknown-loader");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        when(componentRegistry.getExtractor("unknown-extractor")).thenReturn(null);
        when(componentRegistry.getTransformer("unknown-transformer")).thenReturn(null);
        when(componentRegistry.getLoader("unknown-loader")).thenReturn(null);
        when(componentRegistry.getExtractorMap()).thenReturn(Map.of("sql", mock(Extractor.class)));
        when(componentRegistry.getTransformerMap()).thenReturn(Map.of("noop", mock(Transformer.class)));
        when(componentRegistry.getLoaderMap()).thenReturn(Map.of("sql", mock(Loader.class)));
        
        ValidationResult result = jobValidator.validate(job);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("extractorType") && e.getMessage().contains("not registered"));
    }

    @Test
    void validateSqlExtractorWithMissingQuery() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        extractorValidator.validate(job, "sql", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("sourceQuery") && 
            (e.getMessage().contains("required") || e.getMessage().contains("SQL extractor")));
    }

    @Test
    void validateSqlExtractorWithValidQuery() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("query", "SELECT * FROM table");
        EtlJob job = new EtlJob("test-job", "SELECT * FROM table", null, params);
        
        ValidationResult result = new ValidationResult();
        extractorValidator.validate(job, "sql", result);
        
        assertThat(result.hasErrors()).isFalse();
    }

    @Test
    void validateSqlExtractorWithInvalidBatchSize() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("query", "SELECT * FROM table");
        params.put("streamBatchSize", "not-a-number");
        EtlJob job = new EtlJob("test-job", "SELECT * FROM table", null, params);
        
        ValidationResult result = new ValidationResult();
        extractorValidator.validate(job, "sql", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("streamBatchSize") && e.getMessage().contains("must be a number"));
    }

    @Test
    void validateSqlExtractorWithPartitionColumnButNoPartitions() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("query", "SELECT * FROM table");
        params.put("partitionColumn", "bucket");
        EtlJob job = new EtlJob("test-job", "SELECT * FROM table", null, params);
        
        ValidationResult result = new ValidationResult();
        extractorValidator.validate(job, "sql", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("partitions") && e.getMessage().contains("required"));
    }

    @Test
    void validateSqlExtractorWithPartitionColumnAndInvalidPartitions() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("query", "SELECT * FROM table");
        params.put("partitionColumn", "bucket");
        params.put("partitions", 1);
        EtlJob job = new EtlJob("test-job", "SELECT * FROM table", null, params);
        
        ValidationResult result = new ValidationResult();
        extractorValidator.validate(job, "sql", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("partitions") && e.getMessage().contains("greater than 1"));
    }

    @Test
    void validateKafkaExtractorWithMissingTopic() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "kafka");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        extractorValidator.validate(job, "kafka", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("topic") && 
            (e.getMessage().contains("required") || e.getMessage().contains("Kafka extractor")));
    }

    @Test
    void validateKafkaExtractorWithMissingTimestamps() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "kafka");
        params.put("topic", "test-topic");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        extractorValidator.validate(job, "kafka", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("startTimestamp") && 
            (e.getMessage().contains("required") || e.getMessage().contains("Kafka extractor")));
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("endTimestamp") && 
            (e.getMessage().contains("required") || e.getMessage().contains("Kafka extractor")));
    }

    @Test
    void validateKafkaExtractorWithInvalidTimestampTypes() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "kafka");
        params.put("topic", "test-topic");
        params.put("startTimestamp", "not-a-number");
        params.put("endTimestamp", "not-a-number");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        extractorValidator.validate(job, "kafka", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("startTimestamp") && e.getMessage().contains("must be a number"));
    }

    @Test
    void validateKafkaExtractorWithStartTimestampAfterEndTimestamp() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "kafka");
        params.put("topic", "test-topic");
        params.put("startTimestamp", 2000L);
        params.put("endTimestamp", 1000L);
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        extractorValidator.validate(job, "kafka", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("startTimestamp") && e.getMessage().contains("before"));
    }

    @Test
    void validateKafkaExtractorWithValidParameters() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "kafka");
        params.put("topic", "test-topic");
        params.put("startTimestamp", 1000L);
        params.put("endTimestamp", 2000L);
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        extractorValidator.validate(job, "kafka", result);
        
        assertThat(result.hasErrors()).isFalse();
    }

    @Test
    void validateRecordToAvroTransformerWithMissingSchema() {
        Map<String, Object> params = new HashMap<>();
        params.put("transformerType", "record-to-avro");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        transformerValidator.validate(job, "record-to-avro", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("avroSchema") && 
            (e.getMessage().contains("required") || e.getMessage().contains("record-to-avro")));
    }

    @Test
    void validateRecordToAvroTransformerWithInvalidSchema() {
        Map<String, Object> params = new HashMap<>();
        params.put("transformerType", "record-to-avro");
        params.put("avroSchema", "invalid json schema");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        transformerValidator.validate(job, "record-to-avro", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("avroSchema") && e.getMessage().contains("invalid"));
    }

    @Test
    void validateRecordToAvroTransformerWithValidSchema() {
        String validSchema = """
            {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"}
                ]
            }
            """;
        Map<String, Object> params = new HashMap<>();
        params.put("transformerType", "record-to-avro");
        params.put("avroSchema", validSchema);
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        transformerValidator.validate(job, "record-to-avro", result);
        
        assertThat(result.hasErrors()).isFalse();
    }

    @Test
    void validateNoopTransformer() {
        Map<String, Object> params = new HashMap<>();
        params.put("transformerType", "noop");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        transformerValidator.validate(job, "noop", result);
        
        assertThat(result.hasErrors()).isFalse();
    }

    @Test
    void validateSqlLoaderWithMissingTargetTable() {
        Map<String, Object> params = new HashMap<>();
        params.put("loaderType", "sql");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        loaderValidator.validate(job, "sql", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("targetTable") && 
            (e.getMessage().contains("required") || e.getMessage().contains("SQL loader")));
    }

    @Test
    void validateSqlLoaderWithValidTargetTable() {
        Map<String, Object> params = new HashMap<>();
        params.put("loaderType", "sql");
        EtlJob job = new EtlJob("test-job", null, "target_table", params);
        
        ValidationResult result = new ValidationResult();
        loaderValidator.validate(job, "sql", result);
        
        assertThat(result.hasErrors()).isFalse();
    }

    @Test
    void validateKafkaLoaderWithMissingTopic() {
        Map<String, Object> params = new HashMap<>();
        params.put("loaderType", "kafka");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        loaderValidator.validate(job, "kafka", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("topic") && 
            (e.getMessage().contains("required") || e.getMessage().contains("Kafka loader")));
    }

    @Test
    void validateKafkaLoaderWithInvalidFormat() {
        Map<String, Object> params = new HashMap<>();
        params.put("loaderType", "kafka");
        params.put("topic", "test-topic");
        params.put("format", "invalid-format");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        loaderValidator.validate(job, "kafka", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> 
            e.getField().equals("format") && e.getMessage().contains("string") && 
            e.getMessage().contains("avro"));
    }

    @Test
    void validateKafkaLoaderWithValidParameters() {
        Map<String, Object> params = new HashMap<>();
        params.put("loaderType", "kafka");
        params.put("topic", "test-topic");
        params.put("format", "avro");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        loaderValidator.validate(job, "kafka", result);
        
        assertThat(result.hasErrors()).isFalse();
    }

    @Test
    void validateCompleteJobSuccessfully() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("transformerType", "noop");
        params.put("loaderType", "sql");
        params.put("query", "SELECT * FROM table");
        EtlJob job = new EtlJob("test-job", "SELECT * FROM table", "target_table", params);
        
        Extractor mockExtractor = mock(Extractor.class);
        when(mockExtractor.getType()).thenReturn("sql");
        Transformer mockTransformer = mock(Transformer.class);
        when(mockTransformer.getType()).thenReturn("noop");
        Loader mockLoader = mock(Loader.class);
        when(mockLoader.getType()).thenReturn("sql");
        
        when(componentRegistry.getExtractor("sql")).thenReturn(mockExtractor);
        when(componentRegistry.getTransformer("noop")).thenReturn(mockTransformer);
        when(componentRegistry.getLoader("sql")).thenReturn(mockLoader);
        when(componentRegistry.getExtractorMap()).thenReturn(Map.of("sql", mockExtractor));
        when(componentRegistry.getTransformerMap()).thenReturn(Map.of("noop", mockTransformer));
        when(componentRegistry.getLoaderMap()).thenReturn(Map.of("sql", mockLoader));
        
        ValidationResult result = jobValidator.validate(job);
        
        assertThat(result.hasErrors()).isFalse();
    }

    @Test
    void validateOrThrowShouldThrowOnErrors() {
        EtlJob job = new EtlJob(null, null, null, Map.of());
        
        assertThatThrownBy(() -> jobValidator.validateOrThrow(job))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Job validation failed");
    }

    @Test
    void validateOrThrowShouldNotThrowOnSuccess() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("transformerType", "noop");
        params.put("loaderType", "sql");
        params.put("query", "SELECT * FROM table");
        EtlJob job = new EtlJob("test-job", "SELECT * FROM table", "target_table", params);
        
        Extractor mockExtractor = mock(Extractor.class);
        when(mockExtractor.getType()).thenReturn("sql");
        Transformer mockTransformer = mock(Transformer.class);
        when(mockTransformer.getType()).thenReturn("noop");
        Loader mockLoader = mock(Loader.class);
        when(mockLoader.getType()).thenReturn("sql");
        
        when(componentRegistry.getExtractor("sql")).thenReturn(mockExtractor);
        when(componentRegistry.getTransformer("noop")).thenReturn(mockTransformer);
        when(componentRegistry.getLoader("sql")).thenReturn(mockLoader);
        when(componentRegistry.getExtractorMap()).thenReturn(Map.of("sql", mockExtractor));
        when(componentRegistry.getTransformerMap()).thenReturn(Map.of("noop", mockTransformer));
        when(componentRegistry.getLoaderMap()).thenReturn(Map.of("sql", mockLoader));
        
        // Should not throw
        jobValidator.validateOrThrow(job);
    }

    @Test
    void validateUnknownComponentTypes() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "unknown");
        params.put("transformerType", "unknown");
        params.put("loaderType", "unknown");
        EtlJob job = new EtlJob("test-job", null, null, params);
        
        ValidationResult result = new ValidationResult();
        extractorValidator.validate(job, "unknown", result);
        transformerValidator.validate(job, "unknown", result);
        loaderValidator.validate(job, "unknown", result);
        
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> e.getMessage().contains("Unknown extractor type"));
        assertThat(result.getErrors()).anyMatch(e -> e.getMessage().contains("Unknown transformer type"));
        assertThat(result.getErrors()).anyMatch(e -> e.getMessage().contains("Unknown loader type"));
    }
}

