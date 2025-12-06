package ru.pospelov.etl.engine.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.ExtractionException;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.exception.TransformationException;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetrics;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.steps.extractor.Extractor;
import ru.pospelov.etl.engine.steps.loader.Loader;
import ru.pospelov.etl.engine.steps.transformer.Transformer;
import ru.pospelov.etl.engine.validation.JobValidator;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class EtlPipelineFactory {

    private final EtlComponentRegistry registry;
    private final DeadLetterQueue deadLetterQueue;
    private final EtlMetricsCollector metricsCollector;
    private final JobValidator jobValidator;

    public EtlPipelineFactory(
            EtlComponentRegistry registry,
            DeadLetterQueue deadLetterQueue,
            EtlMetricsCollector metricsCollector,
            JobValidator jobValidator
    ) {
        this.registry = registry;
        this.deadLetterQueue = deadLetterQueue;
        this.metricsCollector = metricsCollector;
        this.jobValidator = jobValidator;
    }

    public EtlPipeline create(EtlJob job) {
        // Validate job parameters before creating pipeline
        jobValidator.validateOrThrow(job);

        String extractorType = requireType(job, "extractorType");
        String transformerType = requireType(job, "transformerType");
        String loaderType = requireType(job, "loaderType");

        Extractor extractor = requireExtractor(job, extractorType);
        Transformer transformer = requireTransformer(job, transformerType);
        Loader loader = requireLoader(job, loaderType);

        return new StreamingEtlPipeline(extractor, transformer, loader, deadLetterQueue, metricsCollector);
    }

    private static String requireType(EtlJob job, String key) {
        Object value = job.getParam(key);
        if (value == null) {
            throw new IllegalArgumentException("Job '%s' is missing required parameter '%s'".formatted(job.getJobId(), key));
        }
        return value.toString();
    }

    private Extractor requireExtractor(EtlJob job, String type) {
        Extractor extractor = registry.getExtractor(type);
        if (extractor == null) {
            throw new ExtractionException("Extractor '%s' is not registered".formatted(type), job.getJobId());
        }
        return extractor;
    }

    private Transformer requireTransformer(EtlJob job, String type) {
        Transformer transformer = registry.getTransformer(type);
        if (transformer == null) {
            throw new TransformationException("Transformer '%s' is not registered".formatted(type), job.getJobId(), null);
        }
        return transformer;
    }

    private Loader requireLoader(EtlJob job, String type) {
        Loader loader = registry.getLoader(type);
        if (loader == null) {
            throw new LoadingException("Loader '%s' is not registered".formatted(type), job.getJobId(), null);
        }
        return loader;
    }

    /**
     * Streaming ETL Pipeline that processes data in batches.
     * Instead of loading all data into memory at once, it processes data batch-by-batch,
     * significantly reducing memory footprint for large datasets.
     */
    private static final class StreamingEtlPipeline implements EtlPipeline {

        private final Logger pipelineLog = LoggerFactory.getLogger(StreamingEtlPipeline.class);

        private final Extractor extractor;
        private final Transformer transformer;
        private final Loader loader;
        private final DeadLetterQueue deadLetterQueue;
        private final EtlMetrics metrics;
        private final CancellationToken cancellationToken = new CancellationToken();

        private StreamingEtlPipeline(
                Extractor extractor,
                Transformer transformer,
                Loader loader,
                DeadLetterQueue deadLetterQueue,
                EtlMetrics metrics
        ) {
            this.extractor = Objects.requireNonNull(extractor, "extractor");
            this.transformer = Objects.requireNonNull(transformer, "transformer");
            this.loader = Objects.requireNonNull(loader, "loader");
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

            // Очищаем старые метрики при повторном запуске
            if (metrics instanceof EtlMetricsCollector) {
                ((EtlMetricsCollector) metrics).clear(job.getJobId());
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
                metrics.onJobStatusChanged(job, EtlJobStatus.EXTRACTING);
                metrics.onExtractStart(job);
                pipelineLog.info("Job '{}' started: extractor={}, transformer={}, loader={}",
                        job.getJobId(), extractor.getType(), transformer.getType(), loader.getType());

                // Streaming pipeline: extract -> transform -> load in batches
                // ОПТИМИЗАЦИЯ: Для больших объёмов данных обновляем метрики реже
                // Для малых объёмов (тесты) обновляем каждый batch для корректности
                extractor.extract(job, extractedBatch -> {
                    try {
                        cancellationToken.checkCancellation();

                        int batchNum = batchCounter.incrementAndGet();
                        int extractedCount = extractedBatch.size();
                        totalExtracted.addAndGet(extractedCount);

                        // Обновляем метрики:
                        // - Всегда для первых 10 батчей (для тестов и корректности)
                        // - Каждые 10 батчей для больших объёмов (оптимизация)
                        boolean shouldUpdateMetrics = (batchNum <= 10) || (batchNum % 10 == 0);
                        
                        if (shouldUpdateMetrics) {
                            metrics.onExtractComplete(job, totalExtracted.get(), 0);
                        }

                        if (pipelineLog.isDebugEnabled() || batchNum % 10 == 0) {
                            pipelineLog.info("Job '{}' batch #{}: extracted {} records (total: {})",
                                    job.getJobId(), batchNum, extractedCount, totalExtracted.get());
                        }

                        // Transform batch
                        long transformStartNanos = System.nanoTime();
                        // Статус TRANSFORMING вызываем только для первого батча
                        // (для тестов и корректности статусов)
                        if (batchNum == 1) {
                            metrics.onJobStatusChanged(job, EtlJobStatus.TRANSFORMING);
                        }
                        metrics.onTransformStart(job, extractedCount);

                        Collection<EtlRecord> transformedBatch = transformBatch(job, extractedBatch);

                        cancellationToken.checkCancellation();

                        int transformedCount = transformedBatch.size();
                        totalTransformed.addAndGet(transformedCount);
                        long transformDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - transformStartNanos);
                        totalTransformDurationMillis.addAndGet(transformDurationMillis);

                        if (shouldUpdateMetrics) {
                            metrics.onTransformComplete(job, totalTransformed.get(), totalTransformDurationMillis.get());
                        }

                        if (pipelineLog.isDebugEnabled()) {
                            pipelineLog.debug("Job '{}' batch #{}: transformed {} records in {}ms",
                                    job.getJobId(), batchNum, transformedCount, transformDurationMillis);
                        }

                        // Load batch
                        if (!transformedBatch.isEmpty()) {
                            long loadStartNanos = System.nanoTime();
                            // Статус LOADING вызываем только для первого батча
                            if (batchNum == 1) {
                                metrics.onJobStatusChanged(job, EtlJobStatus.LOADING);
                            }
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
                                        job.getJobId(), batchNum, transformedCount, loadDurationMillis);
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

                // Report final metrics with actual totals
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
                long extractTotalMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - extractStartNanos);

                // Финальное обновление всех метрик с точными значениями
                metrics.onExtractComplete(job, totalExtracted.get(), extractTotalMillis);
                metrics.onTransformComplete(job, totalTransformed.get(), totalTransformDurationMillis.get());
                metrics.onLoadComplete(job, totalLoaded.get(), totalLoadDurationMillis.get());

                metrics.onJobStatusChanged(job, EtlJobStatus.COMPLETED);

                pipelineLog.info("Job '{}' completed successfully: extracted={}, transformed={}, loaded={}, batches={}, total_time={}ms, throughput={} rec/sec",
                        job.getJobId(), totalExtracted.get(), totalTransformed.get(), totalLoaded.get(),
                        batchCounter.get(), elapsedMillis, calculateThroughput(totalLoaded.get(), elapsedMillis));

            } catch (CancellationException e) {
                metrics.onJobStatusChanged(job, EtlJobStatus.CANCELLED);
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
                pipelineLog.warn("Job '{}' was cancelled after {}ms: extracted={}, transformed={}, loaded={}",
                        job.getJobId(), elapsedMillis, totalExtracted.get(), totalTransformed.get(), totalLoaded.get());
                throw e;
            } catch (RuntimeException e) {
                if (cancellationToken.isCancelled()) {
                    metrics.onJobStatusChanged(job, EtlJobStatus.CANCELLED);
                    throw new CancellationException("Execution was cancelled", e);
                }
                metrics.onJobStatusChanged(job, EtlJobStatus.FAILED);
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
                pipelineLog.error("Job '{}' failed after {}ms: extracted={}, transformed={}, loaded={}, error={}",
                        job.getJobId(), elapsedMillis, totalExtracted.get(), totalTransformed.get(),
                        totalLoaded.get(), e.getMessage(), e);
                throw e;
            }
        }

        private long calculateThroughput(int records, long milliseconds) {
            if (milliseconds == 0) {
                return 0;
            }
            return (records * 1000L) / milliseconds;
        }

        private Collection<EtlRecord> transformBatch(EtlJob job, Collection<EtlRecord> batch) {
            if (batch.isEmpty()) {
                return batch;
            }

            try {
                return transformer.transform(batch, job);
            } catch (CancellationException e) {
                throw e;
            } catch (EtlException e) {
                if (cancellationToken.isCancelled()) {
                    throw new CancellationException("Execution was cancelled during transformation", e);
                }
                pipelineLog.error("Job '{}' transformation error: {} - {}",
                        job.getJobId(), e.getClass().getSimpleName(), e.getMessage());
                metrics.onError(job, e);
                deadLetterQueue.publish(e);
                throw e;
            } catch (Exception e) {
                if (cancellationToken.isCancelled()) {
                    throw new CancellationException("Execution was cancelled during transformation", e);
                }
                TransformationException wrapped = new TransformationException(
                        "Failed to transform batch",
                        job.getJobId(),
                        null,
                        EtlErrorSeverity.CRITICAL,
                        e
                );
                pipelineLog.error("Job '{}' transformation failed: {}", job.getJobId(), e.getMessage(), e);
                metrics.onError(job, wrapped);
                deadLetterQueue.publish(wrapped);
                throw wrapped;
            }
        }

        private void loadBatch(EtlJob job, Collection<EtlRecord> batch) {
            if (batch.isEmpty()) {
                return;
            }

            try {
                cancellationToken.checkCancellation();
                loader.load(batch, job);
                cancellationToken.checkCancellation();
            } catch (CancellationException e) {
                throw e;
            } catch (EtlException e) {
                if (cancellationToken.isCancelled()) {
                    throw new CancellationException("Execution was cancelled during loading", e);
                }
                pipelineLog.error("Job '{}' loading error: {} - {}",
                        job.getJobId(), e.getClass().getSimpleName(), e.getMessage());
                metrics.onError(job, e);
                deadLetterQueue.publish(e);
                throw e;
            } catch (Exception e) {
                if (cancellationToken.isCancelled()) {
                    throw new CancellationException("Execution was cancelled during loading", e);
                }
                LoadingException wrapped = new LoadingException(
                        "Failed to load batch",
                        job.getJobId(),
                        null,
                        EtlErrorSeverity.CRITICAL,
                        e
                );
                pipelineLog.error("Job '{}' loading failed: {}", job.getJobId(), e.getMessage(), e);
                metrics.onError(job, wrapped);
                deadLetterQueue.publish(wrapped);
                throw wrapped;
            }
        }
    }
}
