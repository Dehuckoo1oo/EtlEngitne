package ru.pospelov.etl.engine;

import org.junit.jupiter.api.Test;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.config.loader.JdbcLoaderConfig;
import ru.pospelov.etl.engine.config.transformer.NoopTransformerConfig;
import ru.pospelov.etl.engine.pipeline.EtlComponentFactory;
import ru.pospelov.etl.engine.pipeline.EtlPipeline;
import ru.pospelov.etl.engine.pipeline.EtlPipelineFactory;
import ru.pospelov.etl.engine.support.InMemoryDeadLetterQueue;
import ru.pospelov.etl.engine.exception.CancellationException;
import ru.pospelov.etl.engine.metrics.EtlJobMetricsSnapshot;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EtlPipelineCancellationTest {

    @Test
    void cancelledBeforeExecutionResetsOnNewRun() {
        EtlMetricsCollector metricsCollector = new EtlMetricsCollector();
        EtlComponentFactory componentFactory = mock(EtlComponentFactory.class);
        InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();

        EtlJob job = createJob("cancel-before");
        stubSingleBatchExtract(componentFactory, List.of(), job);
        when(componentFactory.transform(any(), any()))
                .thenAnswer(invocation -> invocation.<Collection<EtlRecord>>getArgument(1));
        doNothing().when(componentFactory).load(any(), any());

        EtlPipeline pipeline = new EtlPipelineFactory(componentFactory, deadLetterQueue, metricsCollector).createStreamingEtlPipeline();

        pipeline.cancel();
        assertThat(pipeline.isCancelled()).isTrue();

        pipeline.run(job);
        assertThat(pipeline.isCancelled()).isFalse();
    }

    @Test
    void cancelledDuringTransformStopsExecution() throws InterruptedException {
        EtlMetricsCollector metricsCollector = new EtlMetricsCollector();
        EtlComponentFactory componentFactory = mock(EtlComponentFactory.class);
        InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();

        EtlJob job = createJob("cancel-during-transform");
        List<EtlRecord> batch = createSampleBatch();
        stubSingleBatchExtract(componentFactory, batch, job);

        CountDownLatch transformStarted = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        AtomicReference<EtlPipeline> pipelineRef = new AtomicReference<>();

        when(componentFactory.transform(any(), any())).thenAnswer(invocation -> {
            transformStarted.countDown();
            Thread.sleep(100);
            if (pipelineRef.get().isCancelled()) {
                throw new CancellationException("Cancelled during transform");
            }
            return invocation.<Collection<EtlRecord>>getArgument(1);
        });
        doNothing().when(componentFactory).load(any(), any());

        EtlPipeline pipeline = new EtlPipelineFactory(componentFactory, deadLetterQueue, metricsCollector).createStreamingEtlPipeline();
        pipelineRef.set(pipeline);

        Thread execution = new Thread(() -> {
            try {
                pipeline.run(job);
            } catch (CancellationException e) {
                cancelled.countDown();
            }
        });

        execution.start();
        transformStarted.await(1, TimeUnit.SECONDS);
        Thread.sleep(40);
        pipeline.cancel();

        assertThat(cancelled.await(2, TimeUnit.SECONDS)).isTrue();
        execution.join(1_000);

        EtlJobMetricsSnapshot snapshot = metricsCollector.getSnapshot(job.jobId()).orElseThrow();
        assertThat(snapshot.status()).isEqualTo(EtlJobStatus.CANCELLED);
        assertThat(snapshot.transformedRecords()).isZero();
    }

    @Test
    void cancelledStatusIsReflectedInMetrics() throws InterruptedException {
        EtlMetricsCollector metricsCollector = new EtlMetricsCollector();
        EtlComponentFactory componentFactory = mock(EtlComponentFactory.class);
        InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();

        EtlJob job = createJob("cancel-status");
        stubSingleBatchExtract(componentFactory, createSampleBatch(), job);

        CountDownLatch transformStarted = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        AtomicReference<EtlPipeline> pipelineRef = new AtomicReference<>();

        when(componentFactory.transform(any(), any())).thenAnswer(invocation -> {
            transformStarted.countDown();
            Thread.sleep(80);
            if (pipelineRef.get().isCancelled()) {
                throw new CancellationException("Cancelled");
            }
            return invocation.<Collection<EtlRecord>>getArgument(1);
        });
        doNothing().when(componentFactory).load(any(), any());

        EtlPipeline pipeline = new EtlPipelineFactory(componentFactory, deadLetterQueue, metricsCollector).createStreamingEtlPipeline();
        pipelineRef.set(pipeline);

        Thread execution = new Thread(() -> {
            try {
                pipeline.run(job);
            } catch (CancellationException e) {
                cancelled.countDown();
            }
        });

        execution.start();
        transformStarted.await(1, TimeUnit.SECONDS);
        Thread.sleep(30);
        pipeline.cancel();
        execution.join(1_000);

        assertThat(cancelled.getCount()).isEqualTo(0);

        EtlJobMetricsSnapshot snapshot = metricsCollector.getSnapshot(job.jobId()).orElseThrow();
        assertThat(snapshot.status()).isEqualTo(EtlJobStatus.CANCELLED);
    }

    @Test
    void cancellationCanBeResetOnNewExecution() throws InterruptedException {
        EtlMetricsCollector metricsCollector = new EtlMetricsCollector();
        EtlComponentFactory componentFactory = mock(EtlComponentFactory.class);
        InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();

        stubSingleBatchExtract(componentFactory, createSampleBatch(), null);

        AtomicReference<EtlPipeline> pipelineRef = new AtomicReference<>();
        AtomicInteger transformInvocations = new AtomicInteger(0);

        when(componentFactory.transform(any(), any())).thenAnswer(invocation -> {
            int call = transformInvocations.incrementAndGet();
            if (call == 1) {
                Thread.sleep(80);
                if (pipelineRef.get().isCancelled()) {
                    throw new CancellationException("Cancelled first run");
                }
            }
            return invocation.<Collection<EtlRecord>>getArgument(1);
        });
        doNothing().when(componentFactory).load(any(), any());

        EtlJob job1 = createJob("cancel-reset-1");
        EtlJob job2 = createJob("cancel-reset-2");

        EtlPipeline pipeline = new EtlPipelineFactory(componentFactory, deadLetterQueue, metricsCollector).createStreamingEtlPipeline();
        pipelineRef.set(pipeline);

        CountDownLatch cancelled = new CountDownLatch(1);
        Thread firstRun = new Thread(() -> {
            try {
                pipeline.run(job1);
            } catch (CancellationException e) {
                cancelled.countDown();
            }
        });

        firstRun.start();
        Thread.sleep(30);
        pipeline.cancel();
        firstRun.join(1_000);

        assertThat(cancelled.getCount()).isEqualTo(0);
        EtlJobMetricsSnapshot cancelledSnapshot = metricsCollector.getSnapshot(job1.jobId()).orElseThrow();
        assertThat(cancelledSnapshot.status()).isEqualTo(EtlJobStatus.CANCELLED);

        pipeline.run(job2);

        EtlJobMetricsSnapshot successSnapshot = metricsCollector.getSnapshot(job2.jobId()).orElseThrow();
        assertThat(successSnapshot.status()).isEqualTo(EtlJobStatus.COMPLETED);
        assertThat(pipeline.isCancelled()).isFalse();
    }

    private void stubSingleBatchExtract(EtlComponentFactory componentFactory, List<EtlRecord> batch, EtlJob job) {
        doAnswer(invocation -> {
            Consumer<ru.pospelov.etl.engine.model.EtlBatch> consumer = invocation.getArgument(1);
            consumer.accept(new ru.pospelov.etl.engine.model.EtlBatch(batch, null));
            return null;
        }).when(componentFactory).extract(
                job == null ? any(EtlJob.class) : org.mockito.ArgumentMatchers.eq(job),
                any()
        );
    }

    private List<EtlRecord> createSampleBatch() {
        return List.of(
                new EtlRecord(Instant.now(), "test", 1L),
                new EtlRecord(Instant.now(), "test", 2L)
        );
    }

    private EtlJob createJob(String jobId) {
        JdbcExtractorConfig extractorConfig = new JdbcExtractorConfig(
                Optional.of("SELECT 1"),
                Optional.empty(), // table
                Optional.empty(), // partitionColumn
                1, // partitions
                Optional.empty(), // keyColumn
                1, // threads
                1_000 // streamBatchSize
        );
        NoopTransformerConfig transformerConfig = new NoopTransformerConfig();
        JdbcLoaderConfig loaderConfig = new JdbcLoaderConfig("target_table", 1_000);
        return new EtlJob(jobId, extractorConfig, transformerConfig, loaderConfig);
    }
}
