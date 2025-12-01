package ru.pospelov.etl.engine.metrics;

import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

/**
 * Callback interface for observing ETL pipeline metrics and lifecycle events.
 */
public interface EtlMetrics {

    void onJobStatusChanged(EtlJob job, EtlJobStatus status);

    void onExtractStart(EtlJob job);

    void onExtractComplete(EtlJob job, int extractedRecords, long durationMillis);

    void onTransformStart(EtlJob job, int inputRecords);

    void onTransformComplete(EtlJob job, int outputRecords, long durationMillis);

    void onLoadStart(EtlJob job, int inputRecords);

    void onLoadComplete(EtlJob job, int loadedRecords, long durationMillis);

    void onRecordProcessed(EtlJob job, EtlRecord record);

    void onError(EtlJob job, EtlException exception);
}

