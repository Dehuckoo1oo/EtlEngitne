package ru.pospelov.etl.engine;

import org.junit.jupiter.api.Test;
import ru.pospelov.etl.engine.exception.CancellationException;
import ru.pospelov.etl.engine.engine.DeadLetterQueue;
import ru.pospelov.etl.engine.engine.EtlComponentRegistry;
import ru.pospelov.etl.engine.engine.EtlPipeline;
import ru.pospelov.etl.engine.engine.EtlPipelineFactory;
import ru.pospelov.etl.engine.engine.InMemoryDeadLetterQueue;
import ru.pospelov.etl.engine.validation.JobValidator;
import ru.pospelov.etl.engine.validation.ValidationResult;
import ru.pospelov.etl.engine.metrics.EtlJobMetricsSnapshot;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.steps.extractor.Extractor;
import ru.pospelov.etl.engine.steps.loader.Loader;
import ru.pospelov.etl.engine.steps.transformer.Transformer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class EtlPipelineCancellationTest {

    @Test
    void cancelledBeforeExecutionResetsOnNewRun() {
        EtlPipeline pipeline = createPipeline(
                new InMemoryDeadLetterQueue(),
                new FixedExtractor(),
                new PassThroughTransformer(),
                new RecordingLoader(),
                new EtlMetricsCollector()
        );

        EtlJob job = createJob("cancel-before", "fixed-extractor", "pass-transformer", "recording-loader");
        pipeline.cancel();
        assertThat(pipeline.isCancelled()).isTrue();

        // New run() resets cancellation state, so execution should proceed normally
        pipeline.run(job);
        assertThat(pipeline.isCancelled()).isFalse(); // Reset after successful run
    }

    @Test
    void cancelledDuringTransformStopsExecution() throws InterruptedException {
        EtlMetricsCollector collector = new EtlMetricsCollector();
        EtlPipeline pipeline = createPipeline(
                new InMemoryDeadLetterQueue(),
                new FixedExtractor(),
                new SlowTransformer(100), // 100ms per record
                new RecordingLoader(),
                collector
        );

        EtlJob job = createJob("cancel-during-transform", "fixed-extractor", "slow-transformer", "recording-loader");

        CountDownLatch startedLatch = new CountDownLatch(1);
        CountDownLatch cancelledLatch = new CountDownLatch(1);

        Thread executionThread = new Thread(() -> {
            try {
                startedLatch.countDown();
                pipeline.run(job);
            } catch (CancellationException e) {
                cancelledLatch.countDown();
            }
        });

        executionThread.start();
        startedLatch.await(1, TimeUnit.SECONDS);

        // Wait a bit to let transform start
        Thread.sleep(50);
        pipeline.cancel();

        // Wait for cancellation
        assertThat(cancelledLatch.await(2, TimeUnit.SECONDS)).isTrue();

        EtlJobMetricsSnapshot snapshot = collector.getSnapshot(job.getJobId()).orElseThrow();
        assertThat(snapshot.status()).isEqualTo(EtlJobStatus.CANCELLED);
        assertThat(snapshot.transformedRecords()).isLessThan(2); // Should not process all records
    }

    @Test
    void cancelledStatusIsReflectedInMetrics() throws InterruptedException {
        EtlMetricsCollector collector = new EtlMetricsCollector();
        EtlPipeline pipeline = createPipeline(
                new InMemoryDeadLetterQueue(),
                new FixedExtractor(),
                new SlowTransformer(50), // Slow enough to cancel during execution
                new RecordingLoader(),
                collector
        );

        EtlJob job = createJob("cancel-status", "fixed-extractor", "slow-transformer", "recording-loader");

        CountDownLatch startedLatch = new CountDownLatch(1);
        Thread executionThread = new Thread(() -> {
            try {
                startedLatch.countDown();
                pipeline.run(job);
            } catch (CancellationException e) {
                // Expected
            }
        });

        executionThread.start();
        startedLatch.await(1, TimeUnit.SECONDS);
        Thread.sleep(30); // Let it start processing
        pipeline.cancel();
        executionThread.join(1000);

        EtlJobMetricsSnapshot snapshot = collector.getSnapshot(job.getJobId()).orElseThrow();
        assertThat(snapshot.status()).isEqualTo(EtlJobStatus.CANCELLED);
    }

    @Test
    void isCancelledReturnsTrueAfterCancel() {
        EtlPipeline pipeline = createPipeline(
                new InMemoryDeadLetterQueue(),
                new FixedExtractor(),
                new PassThroughTransformer(),
                new RecordingLoader(),
                new EtlMetricsCollector()
        );

        assertThat(pipeline.isCancelled()).isFalse();
        pipeline.cancel();
        assertThat(pipeline.isCancelled()).isTrue();
    }

    @Test
    void cancellationCanBeResetOnNewExecution() throws InterruptedException {
        EtlMetricsCollector collector = new EtlMetricsCollector();
        EtlPipeline pipeline = createPipeline(
                new InMemoryDeadLetterQueue(),
                new FixedExtractor(),
                new SlowTransformer(100),
                new RecordingLoader(),
                collector
        );

        EtlJob job1 = createJob("cancel-reset-1", "fixed-extractor", "slow-transformer", "recording-loader");

        // Start execution and cancel it
        CountDownLatch startedLatch = new CountDownLatch(1);
        Thread thread1 = new Thread(() -> {
            try {
                startedLatch.countDown();
                pipeline.run(job1);
            } catch (CancellationException e) {
                // Expected
            }
        });
        thread1.start();
        startedLatch.await(1, TimeUnit.SECONDS);
        Thread.sleep(30);
        pipeline.cancel();
        thread1.join(1000);

        EtlJobMetricsSnapshot snapshot1 = collector.getSnapshot(job1.getJobId()).orElseThrow();
        assertThat(snapshot1.status()).isEqualTo(EtlJobStatus.CANCELLED);

        // New execution should reset cancellation and succeed
        EtlJob job2 = createJob("cancel-reset-2", "fixed-extractor", "pass-transformer", "recording-loader");
        pipeline.run(job2); // Should succeed

        EtlJobMetricsSnapshot snapshot2 = collector.getSnapshot(job2.getJobId()).orElseThrow();
        assertThat(snapshot2.status()).isEqualTo(EtlJobStatus.COMPLETED);
    }

    private EtlPipeline createPipeline(
            DeadLetterQueue deadLetterQueue,
            Extractor extractor,
            Transformer transformer,
            Loader loader,
            EtlMetricsCollector metricsCollector
    ) {
        EtlComponentRegistry registry = new EtlComponentRegistry(
                List.of(extractor),
                List.of(transformer),
                List.of(loader)
        );
        JobValidator jobValidator = job -> new ValidationResult();
        EtlPipelineFactory factory = new EtlPipelineFactory(registry, deadLetterQueue, metricsCollector, jobValidator);
        EtlJob templateJob = createJob("template", extractor.getType(), transformer.getType(), loader.getType());
        return factory.create(templateJob);
    }

    private EtlJob createJob(String jobId, String extractorType, String transformerType, String loaderType) {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", extractorType);
        params.put("transformerType", transformerType);
        params.put("loaderType", loaderType);
        return new EtlJob(jobId, null, "target_table", params);
    }

    private static class FixedExtractor implements Extractor {
        @Override
        public void extract(EtlJob job, java.util.function.Consumer<Collection<EtlRecord>> batchConsumer) {
            EtlRecord first = new EtlRecord(Instant.now(), "test", 1L);
            first.put("value", "ok");
            EtlRecord second = new EtlRecord(Instant.now(), "test", 2L);
            second.put("value", "ok");
            batchConsumer.accept(List.of(first, second));
        }

        @Override
        public String getType() {
            return "fixed-extractor";
        }
    }

    private static class SlowTransformer implements Transformer {
        private final long delayMillis;

        SlowTransformer(long delayMillis) {
            this.delayMillis = delayMillis;
        }

        @Override
        public Collection<EtlRecord> transform(Collection<EtlRecord> etlRecords, EtlJob job) {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return etlRecords;
        }

        @Override
        public String getType() {
            return "slow-transformer";
        }
    }

    private static class PassThroughTransformer implements Transformer {
        @Override
        public Collection<EtlRecord> transform(Collection<EtlRecord> etlRecords, EtlJob job) {
            return etlRecords;
        }

        @Override
        public String getType() {
            return "pass-transformer";
        }
    }

    private static class RecordingLoader implements Loader {
        private final List<EtlRecord> loadedRecords = new ArrayList<>();

        @Override
        public void load(Collection<EtlRecord> etlRecords, EtlJob job) {
            loadedRecords.addAll(etlRecords);
        }

        @Override
        public String getType() {
            return "recording-loader";
        }

        List<EtlRecord> getLoadedRecords() {
            return loadedRecords;
        }
    }

}

