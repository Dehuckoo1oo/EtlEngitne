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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Component
public class EtlPipelineFactory {

    private final EtlComponentRegistry registry;
    private final DeadLetterQueue deadLetterQueue;
    private final EtlMetricsCollector metricsCollector;

    public EtlPipelineFactory(
            EtlComponentRegistry registry,
            DeadLetterQueue deadLetterQueue,
            EtlMetricsCollector metricsCollector
    ) {
        this.registry = registry;
        this.deadLetterQueue = deadLetterQueue;
        this.metricsCollector = metricsCollector;
    }

    public EtlPipeline create(EtlJob job) {
        String extractorType = requireType(job, "extractorType");
        String transformerType = requireType(job, "transformerType");
        String loaderType = requireType(job, "loaderType");

        Extractor extractor = requireExtractor(job, extractorType);
        Transformer transformer = requireTransformer(job, transformerType);
        Loader loader = requireLoader(job, loaderType);

        return new StructuredEtlPipeline(extractor, transformer, loader, deadLetterQueue, metricsCollector);
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

    private static final class StructuredEtlPipeline implements EtlPipeline {

        private final Logger pipelineLog = LoggerFactory.getLogger(StructuredEtlPipeline.class);

        private final Extractor extractor;
        private final Transformer transformer;
        private final Loader loader;
        private final DeadLetterQueue deadLetterQueue;
        private final EtlMetrics metrics;
        private final CancellationToken cancellationToken = new CancellationToken();

        private StructuredEtlPipeline(
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
            try {
                cancellationToken.checkCancellation();
                Collection<EtlRecord> extracted = extract(job);
                cancellationToken.checkCancellation();
                Collection<EtlRecord> transformed = transform(job, extracted);
                cancellationToken.checkCancellation();
                load(job, transformed);
                cancellationToken.checkCancellation();
                metrics.onJobStatusChanged(job, EtlJobStatus.COMPLETED);
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

        private Collection<EtlRecord> extract(EtlJob job) {
            cancellationToken.checkCancellation();
            metrics.onJobStatusChanged(job, EtlJobStatus.EXTRACTING);
            metrics.onExtractStart(job);
            long startedAtNanos = System.nanoTime();
            try {
                Collection<EtlRecord> records = extractor.extract(job);
                cancellationToken.checkCancellation();
                Collection<EtlRecord> safeRecords = records == null ? List.of() : records;
                metrics.onExtractComplete(job, safeRecords.size(), elapsedMillis(startedAtNanos));
                return safeRecords;
            } catch (CancellationException e) {
                throw e;
            } catch (EtlException e) {
                if (cancellationToken.isCancelled()) {
                    throw new CancellationException("Execution was cancelled during extraction", e);
                }
                metrics.onError(job, e);
                deadLetterQueue.publish(e);
                throw e;
            } catch (Exception e) {
                if (cancellationToken.isCancelled()) {
                    throw new CancellationException("Execution was cancelled during extraction", e);
                }
                ExtractionException wrapped = new ExtractionException(
                        "Failed to extract records",
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

        private Collection<EtlRecord> transform(EtlJob job, Collection<EtlRecord> records) {
            cancellationToken.checkCancellation();
            Collection<EtlRecord> safeRecords = records == null ? List.of() : records;
            metrics.onJobStatusChanged(job, EtlJobStatus.TRANSFORMING);
            metrics.onTransformStart(job, safeRecords.size());
            long startedAtNanos = System.nanoTime();
            if (safeRecords.isEmpty()) {
                metrics.onTransformComplete(job, 0, elapsedMillis(startedAtNanos));
                return List.of();
            }

            List<EtlRecord> transformed = new ArrayList<>();
            for (EtlRecord record : safeRecords) {
                cancellationToken.checkCancellation(); // Check before processing each record
                try {
                    Collection<EtlRecord> result = transformer.transform(List.of(record), job);
                    if (result != null && !result.isEmpty()) {
                        transformed.addAll(result);
                        result.forEach(created -> metrics.onRecordProcessed(job, created));
                    }
                } catch (CancellationException e) {
                    throw e;
                } catch (EtlException e) {
                    if (cancellationToken.isCancelled()) {
                        throw new CancellationException("Execution was cancelled during transformation", e);
                    }
                    handleStageException(job, e);
                } catch (Exception e) {
                    if (cancellationToken.isCancelled()) {
                        throw new CancellationException("Execution was cancelled during transformation", e);
                    }
                    TransformationException wrapped = new TransformationException(
                            "Failed to transform record with offset %s".formatted(record.getOffset()),
                            job.getJobId(),
                            record,
                            EtlErrorSeverity.CRITICAL,
                            e
                    );
                    handleStageException(job, wrapped);
                }
            }

            cancellationToken.checkCancellation();
            metrics.onTransformComplete(job, transformed.size(), elapsedMillis(startedAtNanos));
            return transformed;
        }

        private void load(EtlJob job, Collection<EtlRecord> records) {
            cancellationToken.checkCancellation();
            Collection<EtlRecord> safeRecords = records == null ? List.of() : records;
            metrics.onJobStatusChanged(job, EtlJobStatus.LOADING);
            metrics.onLoadStart(job, safeRecords.size());
            long startedAtNanos = System.nanoTime();
            if (safeRecords.isEmpty()) {
                metrics.onLoadComplete(job, 0, elapsedMillis(startedAtNanos));
                return;
            }

            try {
                cancellationToken.checkCancellation();
                loader.load(safeRecords, job);
                cancellationToken.checkCancellation();
                metrics.onLoadComplete(job, safeRecords.size(), elapsedMillis(startedAtNanos));
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
                        "Failed to load records",
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

        private void handleStageException(EtlJob job, EtlException exception) {
            metrics.onError(job, exception);
            deadLetterQueue.publish(exception);
            if (exception.isCritical() || exception.getRecord() == null) {
                throw exception;
            }

            pipelineLog.warn(
                    "Job '{}' - skipping record due to non-critical {} error: {}",
                    job.getJobId(),
                    exception.getStage(),
                    exception.getMessage()
            );
        }

        private static long elapsedMillis(long startedAtNanos) {
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
        }
    }
}