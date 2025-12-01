package ru.pospelov.etl.engine.exception;

/**
 * Classification of ETL errors that determines whether execution can continue.
 */
public enum EtlErrorSeverity {
    CRITICAL,
    NON_CRITICAL
}

