package ru.pospelov.etl.engine.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.pospelov.etl.engine.exception.CancellationException;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.exception.TransformationException;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetrics;
import ru.pospelov.etl.engine.model.EtlBatch;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.support.CancellationToken;
import ru.pospelov.etl.engine.support.DeadLetterQueue;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

final class StreamingEtlPipeline implements EtlPipeline {

    private final Logger pipelineLog = LoggerFactory.getLogger(StreamingEtlPipeline.class);

    private final EtlComponentFactory componentFactory;
    private final DeadLetterQueue deadLetterQueue;
    private final EtlMetrics metrics;
    private final CancellationToken cancellationToken = new CancellationToken();

    StreamingEtlPipeline(
            EtlComponentFactory componentFactory,
            DeadLetterQueue deadLetterQueue,
            EtlMetrics metrics
    ) {
        this.componentFactory = Objects.requireNonNull(componentFactory, "componentFactory");
        this.deadLetterQueue = Objects.requireNonNull(deadLetterQueue, "deadLetterQueue");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    @Override
    public void cancel() {
        cancellationToken.cancel();
    }

    @Override
    public boolean isCancelled() {
        return cancellationToken.isCancelled();
    }

    @Override
    public void run(EtlJob job) {
        Objects.requireNonNull(job, "job");
        cancellationToken.reset(); // Reset cancellation state for new execution

        if (metrics instanceof EtlMetricsCollector) {
            ((EtlMetricsCollector) metrics).clear(job.jobId());
        }

        metrics.onJobStatusChanged(job, EtlJobStatus.RUNNING);

        final AtomicInteger totalExtracted = new AtomicInteger(0);
        final AtomicInteger totalTransformed = new AtomicInteger(0);
        final AtomicInteger totalLoaded = new AtomicInteger(0);
        final AtomicInteger batchCounter = new AtomicInteger(0);
        final AtomicLong totalTransformDurationMillis = new AtomicLong(0);
        final AtomicLong totalLoadDurationMillis = new AtomicLong(0);
        final long startTimeNanos = System.nanoTime();
        final long extractStartNanos = System.nanoTime();

        try {
            cancellationToken.checkCancellation();
            metrics.onExtractStart(job);
            pipelineLog.info("Job '{}' started: extractor={}, transformer={}, loader={}",
                    job.jobId(), job.extractorConfig().type(), job.transformerConfig().type(), job.loaderConfig().type());

            // Use componentFactory for type-safe extraction
            componentFactory.extract(job, extractedBatch -> {
                try {
                    cancellationToken.checkCancellation();

                    int batchNum = batchCounter.incrementAndGet();
                    int extractedCount = extractedBatch.getRecords().size();
                    totalExtracted.addAndGet(extractedCount);

                    boolean shouldUpdateMetrics = (batchNum <= 10) || (batchNum % 10 == 0);

                    if (shouldUpdateMetrics) {
                        metrics.onExtractComplete(job, totalExtracted.get(), 0);
                    }

                    if (pipelineLog.isDebugEnabled() || batchNum % 10 == 0) {
                        pipelineLog.info("Job '{}' batch #{}: extracted {} records (total: {})",
                                job.jobId(), batchNum, extractedCount, totalExtracted.get());
                    }

                    long transformStartNanos = System.nanoTime();
                    metrics.onTransformStart(job, extractedCount);

                    EtlBatch transformedBatch = transformBatch(job, extractedBatch);

                    cancellationToken.checkCancellation();

                    int transformedCount = transformedBatch.getRecords().size();
                    totalTransformed.addAndGet(transformedCount);
                    long transformDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - transformStartNanos);
                    totalTransformDurationMillis.addAndGet(transformDurationMillis);

                    if (shouldUpdateMetrics) {
                        metrics.onTransformComplete(job, totalTransformed.get(), totalTransformDurationMillis.get());
                    }

                    if (pipelineLog.isDebugEnabled()) {
                        pipelineLog.debug("Job '{}' batch #{}: transformed {} records in {}ms",
                                job.jobId(), batchNum, transformedCount, transformDurationMillis);
                    }

                    if (!transformedBatch.getRecords().isEmpty()) {
                        long loadStartNanos = System.nanoTime();
                        metrics.onLoadStart(job, transformedCount);

                        loadBatch(job, transformedBatch);

                        totalLoaded.addAndGet(transformedCount);
                        long loadDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - loadStartNanos);
                        totalLoadDurationMillis.addAndGet(loadDurationMillis);

                        if (shouldUpdateMetrics) {
                            metrics.onLoadComplete(job, totalLoaded.get(), totalLoadDurationMillis.get());
                        }

                        if (pipelineLog.isDebugEnabled()) {
                            pipelineLog.debug("Job '{}' batch #{}: loaded {} records in {}ms",
                                    job.jobId(), batchNum, transformedCount, loadDurationMillis);
                        }
                    }
                    cancellationToken.checkCancellation();
                } catch (CancellationException e) {
                    throw e;
                } catch (Exception e) {
                    if (cancellationToken.isCancelled()) {
                        throw new CancellationException("Execution was cancelled during batch processing", e);
                    }
                    throw e;
                }
            });

            cancellationToken.checkCancellation();

            long elapsedMillis = elapsedMillis(startTimeNanos);
            long extractTotalMillis = elapsedMillis(extractStartNanos);

            metrics.onExtractComplete(job, totalExtracted.get(), extractTotalMillis);
            metrics.onTransformComplete(job, totalTransformed.get(), totalTransformDurationMillis.get());
            metrics.onLoadComplete(job, totalLoaded.get(), totalLoadDurationMillis.get());

            metrics.onJobStatusChanged(job, EtlJobStatus.COMPLETED);

            pipelineLog.info("Job '{}' completed successfully: extracted={}, transformed={}, loaded={}, batches={}, total_time={}ms, throughput={} rec/sec",
                    job.jobId(), totalExtracted.get(), totalTransformed.get(), totalLoaded.get(),
                    batchCounter.get(), elapsedMillis, calculateThroughput(totalLoaded.get(), elapsedMillis));

        } catch (CancellationException e) {
            handleJobCancellation(job, startTimeNanos, totalExtracted.get(), totalTransformed.get(), totalLoaded.get());
            throw e;
        } catch (RuntimeException e) {
            handleJobFailure(job, startTimeNanos, totalExtracted.get(), totalTransformed.get(), totalLoaded.get(), e);
            throw e;
        }
    }

    private EtlBatch transformBatch(EtlJob job, EtlBatch batch) {
        if (batch.getRecords().isEmpty()) {
            return batch;
        }

        return runStage(
                job,
                "transformation",
                () -> componentFactory.transform(job, batch), // Use componentFactory
                ex -> new TransformationException(
                        "Failed to transform batch",
                        job.jobId(),
                        null,
                        EtlErrorSeverity.CRITICAL,
                        ex
                )
        );
    }

    private void loadBatch(EtlJob job, EtlBatch batch) {
        if (batch.getRecords().isEmpty()) {
            return;
        }

        runStage(
                job,
                "loading",
                () -> {
                    cancellationToken.checkCancellation();
                    componentFactory.load(job, batch); // Use componentFactory
                    cancellationToken.checkCancellation();
                    return null;
                },
                ex -> new LoadingException(
                        "Failed to load batch",
                        job.jobId(),
                        null,
                        EtlErrorSeverity.CRITICAL,
                        ex
                )
        );
    }

    private <T> T runStage(
            EtlJob job,
            String stage,
            Callable<T> action,
            Function<Exception, EtlException> wrapIfNeeded
    ) {
        try {
            cancellationToken.checkCancellation();
            return action.call();
        } catch (CancellationException e) {
            throw e;
        } catch (EtlException e) {
            if (cancellationToken.isCancelled()) {
                throw new CancellationException("Execution was cancelled during " + stage, e);
            }
            handleStageError(job, stage, e);
            throw e;
        } catch (Exception e) {
            if (cancellationToken.isCancelled()) {
                throw new CancellationException("Execution was cancelled during " + stage, e);
            }
            EtlException wrapped = wrapIfNeeded.apply(e);
            handleStageError(job, stage, wrapped);
            throw wrapped;
        }
    }

    private void handleStageError(EtlJob job, String stage, EtlException e) {
        pipelineLog.error("Job '{}' {} error: {} - {}", job.jobId(), stage, e.getClass().getSimpleName(), e.getMessage(), e);
        metrics.onError(job, e);
        deadLetterQueue.publish(e);
    }

    private void handleJobCancellation(EtlJob job, long startTimeNanos, int totalExtracted, int totalTransformed, int totalLoaded) {
        metrics.onJobStatusChanged(job, EtlJobStatus.CANCELLED);
        long elapsedMillis = elapsedMillis(startTimeNanos);
        pipelineLog.warn("Job '{}' was cancelled after {}ms: extracted={}, transformed={}, loaded={}",
                job.jobId(), elapsedMillis, totalExtracted, totalTransformed, totalLoaded);
    }

    private void handleJobFailure(EtlJob job, long startTimeNanos, int totalExtracted, int totalTransformed, int totalLoaded, RuntimeException error) {
        if (cancellationToken.isCancelled()) {
            metrics.onJobStatusChanged(job, EtlJobStatus.CANCELLED);
            throw new CancellationException("Execution was cancelled", error);
        }
        metrics.onJobStatusChanged(job, EtlJobStatus.FAILED);
        long elapsedMillis = elapsedMillis(startTimeNanos);
        pipelineLog.error("Job '{}' failed after {}ms: extracted={}, transformed={}, loaded={}, error={}",
                job.jobId(), elapsedMillis, totalExtracted, totalTransformed, totalLoaded, error.getMessage(), error);
    }

    private long elapsedMillis(long startTimeNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
    }

    private long calculateThroughput(int records, long milliseconds) {
        if (milliseconds == 0) {
            return 0;
        }
        return (records * 1000L) / milliseconds;
    }
}
