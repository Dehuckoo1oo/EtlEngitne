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
            metrics.onJobStatusChanged(job, EtlJobStatus.RUNNING);
            
            final AtomicInteger totalExtracted = new AtomicInteger(0);
            final AtomicInteger totalTransformed = new AtomicInteger(0);
            final AtomicInteger totalLoaded = new AtomicInteger(0);
            final long startTimeNanos = System.nanoTime();
            
            try {
                cancellationToken.checkCancellation();
                metrics.onJobStatusChanged(job, EtlJobStatus.EXTRACTING);
                metrics.onExtractStart(job);
                
                // Streaming pipeline: extract -> transform -> load in batches
                extractor.extract(job, extractedBatch -> {
                    try {
                        cancellationToken.checkCancellation();
                        
                        int extractedCount = extractedBatch.size();
                        totalExtracted.addAndGet(extractedCount);
                        
                        // Transform batch
                        metrics.onJobStatusChanged(job, EtlJobStatus.TRANSFORMING);
                        Collection<EtlRecord> transformedBatch = transformBatch(job, extractedBatch);
                        
                        int transformedCount = transformedBatch.size();
                        totalTransformed.addAndGet(transformedCount);
                        transformedBatch.forEach(record -> metrics.onRecordProcessed(job, record));
                        
                        cancellationToken.checkCancellation();
                        
                        // Load batch
                        if (!transformedBatch.isEmpty()) {
                            metrics.onJobStatusChanged(job, EtlJobStatus.LOADING);
                            loadBatch(job, transformedBatch);
                            totalLoaded.addAndGet(transformedCount);
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
                
                // Report final metrics
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
                metrics.onExtractComplete(job, totalExtracted.get(), elapsedMillis);
                metrics.onTransformComplete(job, totalTransformed.get(), elapsedMillis);
                metrics.onLoadComplete(job, totalLoaded.get(), elapsedMillis);
                
                metrics.onJobStatusChanged(job, EtlJobStatus.COMPLETED);
                
                pipelineLog.info("Job '{}' completed: extracted={}, transformed={}, loaded={}, time={}ms",
                        job.getJobId(), totalExtracted.get(), totalTransformed.get(), totalLoaded.get(), elapsedMillis);
                
            } catch (CancellationException e) {
                metrics.onJobStatusChanged(job, EtlJobStatus.CANCELLED);
                pipelineLog.info("Job '{}' execution was cancelled", job.getJobId());
                throw e;
            } catch (RuntimeException e) {
                if (cancellationToken.isCancelled()) {
                    metrics.onJobStatusChanged(job, EtlJobStatus.CANCELLED);
                    throw new CancellationException("Execution was cancelled", e);
                }
                metrics.onJobStatusChanged(job, EtlJobStatus.FAILED);
                throw e;
            }
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
                metrics.onError(job, wrapped);
                deadLetterQueue.publish(wrapped);
                throw wrapped;
            }
        }
    }
}
