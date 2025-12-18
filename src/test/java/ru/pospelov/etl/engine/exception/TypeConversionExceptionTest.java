package ru.pospelov.etl.engine.exception;

import org.junit.jupiter.api.Test;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TypeConversionException}.
 */
class TypeConversionExceptionTest {

    @Test
    void testSimpleConstructor() {
        TypeConversionException exception = new TypeConversionException(
                "Type mismatch error",
                "job-123",
                EtlStage.LOAD
        );

        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("Type mismatch error"));
        assertEquals("job-123", exception.getJobId());
        assertEquals(EtlStage.LOAD, exception.getStage());
        assertNull(exception.getRecord());
        assertEquals(EtlErrorSeverity.CRITICAL, exception.getSeverity());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructorWithCause() {
        NumberFormatException cause = new NumberFormatException("Invalid number format");
        TypeConversionException exception = new TypeConversionException(
                "Failed to convert string to number",
                "job-456",
                EtlStage.EXTRACT,
                cause
        );

        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("Failed to convert string to number"));
        assertEquals("job-456", exception.getJobId());
        assertEquals(EtlStage.EXTRACT, exception.getStage());
        assertNull(exception.getRecord());
        assertEquals(EtlErrorSeverity.CRITICAL, exception.getSeverity());
        assertSame(cause, exception.getCause());
    }

    @Test
    void testConstructorWithFullContext() {
        EtlRecord record = new EtlRecord(Instant.now(), "partition-0", 12345L);
        record.put("age", "twenty");

        IllegalArgumentException cause = new IllegalArgumentException("Invalid value");
        TypeConversionException exception = new TypeConversionException(
                "Type conversion failed for field 'amount'",
                "job-789",
                EtlStage.LOAD,
                record,
                cause
        );

        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("Type conversion failed for field 'amount'"));
        assertEquals("job-789", exception.getJobId());
        assertEquals(EtlStage.LOAD, exception.getStage());
        assertSame(record, exception.getRecord());
        assertEquals("partition-0", exception.getRecord().getSourcePartition());
        assertEquals(12345L, exception.getRecord().getOffset());
        assertEquals(EtlErrorSeverity.CRITICAL, exception.getSeverity());
        assertSame(cause, exception.getCause());
    }

    @Test
    void testInheritanceFromEtlException() {
        TypeConversionException exception = new TypeConversionException(
                "Test error",
                "job-test",
                EtlStage.TRANSFORM
        );

        // Should be instance of EtlException
        assertTrue(exception instanceof EtlException);
        assertTrue(exception instanceof RuntimeException);
    }

    @Test
    void testSeverityIsAlwaysCritical() {
        // TypeConversionException always has CRITICAL severity
        TypeConversionException ex1 = new TypeConversionException("error1", "job1", EtlStage.EXTRACT);
        TypeConversionException ex2 = new TypeConversionException("error2", "job2", EtlStage.TRANSFORM, new RuntimeException());
        TypeConversionException ex3 = new TypeConversionException("error3", "job3", EtlStage.LOAD, null, new RuntimeException());

        assertEquals(EtlErrorSeverity.CRITICAL, ex1.getSeverity());
        assertEquals(EtlErrorSeverity.CRITICAL, ex2.getSeverity());
        assertEquals(EtlErrorSeverity.CRITICAL, ex3.getSeverity());
    }

    @Test
    void testMessagePreservation() {
        EtlRecord record = new EtlRecord(Instant.now(), "orders-0", 12345L);
        record.put("age", "twenty");

        String detailedMessage = "Cannot convert String to Avro int for field 'age': " +
                "expected int, actual java.lang.String, value=\"twenty\"";

        TypeConversionException exception = new TypeConversionException(
                detailedMessage,
                "sql-to-kafka-001",
                EtlStage.LOAD,
                record,
                null
        );

        String message = exception.getMessage();
        assertTrue(message.contains("Cannot convert String to Avro int"));
        assertTrue(message.contains("field 'age'"));
        assertTrue(message.contains("twenty"));

        // Verify record context
        assertEquals("orders-0", exception.getRecord().getSourcePartition());
        assertEquals(12345L, exception.getRecord().getOffset());
    }

    @Test
    void testDifferentStages() {
        TypeConversionException extractException = new TypeConversionException(
                "Extract stage error",
                "job-1",
                EtlStage.EXTRACT
        );

        TypeConversionException transformException = new TypeConversionException(
                "Transform stage error",
                "job-2",
                EtlStage.TRANSFORM
        );

        TypeConversionException loadException = new TypeConversionException(
                "Load stage error",
                "job-3",
                EtlStage.LOAD
        );

        assertEquals(EtlStage.EXTRACT, extractException.getStage());
        assertEquals(EtlStage.TRANSFORM, transformException.getStage());
        assertEquals(EtlStage.LOAD, loadException.getStage());
    }
}
