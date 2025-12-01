package ru.pospelov.etl.engine.exception;

/**
 * High-level stages of an ETL pipeline used for structured error reporting.
 */
public enum EtlStage {
    EXTRACT,
    TRANSFORM,
    LOAD
}

