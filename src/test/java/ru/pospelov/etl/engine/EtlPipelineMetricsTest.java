package ru.pospelov.etl.engine;

import org.junit.jupiter.api.Test;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.config.loader.JdbcLoaderConfig;
import ru.pospelov.etl.engine.config.transformer.NoopTransformerConfig;
import ru.pospelov.etl.engine.pipeline.EtlComponentFactory;
import ru.pospelov.etl.engine.pipeline.EtlPipeline;
import ru.pospelov.etl.engine.pipeline.EtlPipelineFactory;
import ru.pospelov.etl.engine.support.InMemoryDeadLetterQueue;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.TransformationException;
import ru.pospelov.etl.engine.metrics.EtlJobMetricsSnapshot;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetrics;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EtlPipelineMetricsTest {

    @Test
    void successfulJobProducesMetricsAndStatuses() {
        EtlMetricsCollector collector = new EtlMetricsCollector();
        RecordingMetricsListener listener = new RecordingMetricsListener();
        collector.registerListener(listener);

        InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();
        EtlComponentFactory componentFactory = mock(EtlComponentFactory.class);

        EtlJob job = createJob("metrics-success");
        EtlRecord first = new EtlRecord(Instant.now(), "test", 1L);
        EtlRecord second = new EtlRecord(Instant.now(), "test", 2L);

        doAnswer(invocation -> {
            Consumer<ru.pospelov.etl.engine.model.EtlBatch> consumer = invocation.getArgument(1);
            consumer.accept(new ru.pospelov.etl.engine.model.EtlBatch(List.of(first, second), null));
            return null;
        }).when(componentFactory).extract(eq(job), any());

        when(componentFactory.transform(eq(job), any()))
                .thenAnswer(invocation -> invocation.<ru.pospelov.etl.engine.model.EtlBatch>getArgument(1));
        doNothing().when(componentFactory).load(eq(job), any());

        EtlPipeline pipeline = new EtlPipelineFactory(componentFactory, deadLetterQueue, collector).createStreamingEtlPipeline();
        pipeline.run(job);

        EtlJobMetricsSnapshot snapshot = collector.getSnapshot(job.jobId()).orElseThrow();
        assertThat(snapshot.status()).isEqualTo(EtlJobStatus.COMPLETED);
        assertThat(snapshot.extractedRecords()).isEqualTo(2);
        assertThat(snapshot.transformedRecords()).isEqualTo(2);
        assertThat(snapshot.loadedRecords()).isEqualTo(2);
        assertThat(snapshot.errorCount()).isZero();
        assertThat(deadLetterQueue.getEntries(job.jobId())).isEmpty();

        assertThat(listener.statuses())
                .containsExactly(EtlJobStatus.RUNNING, EtlJobStatus.COMPLETED);
        assertThat(listener.errorCount()).isZero();
    }

    @Test
    void failingTransformerMarksStatusFailedAndCountsError() {
        EtlMetricsCollector collector = new EtlMetricsCollector();
        RecordingMetricsListener listener = new RecordingMetricsListener();
        collector.registerListener(listener);

        InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();
        EtlComponentFactory componentFactory = mock(EtlComponentFactory.class);

        EtlJob job = createJob("metrics-failure");
        EtlRecord first = new EtlRecord(Instant.now(), "test", 1L);
        EtlRecord second = new EtlRecord(Instant.now(), "test", 2L);

        doAnswer(invocation -> {
            Consumer<ru.pospelov.etl.engine.model.EtlBatch> consumer = invocation.getArgument(1);
            consumer.accept(new ru.pospelov.etl.engine.model.EtlBatch(List.of(first, second), null));
            return null;
        }).when(componentFactory).extract(eq(job), any());

        when(componentFactory.transform(eq(job), any()))
                .thenThrow(new TransformationException("boom", job.jobId(), second, EtlErrorSeverity.CRITICAL));
        doNothing().when(componentFactory).load(eq(job), any());

        EtlPipeline pipeline = new EtlPipelineFactory(componentFactory, deadLetterQueue, collector).createStreamingEtlPipeline();

        assertThatThrownBy(() -> pipeline.run(job))
                .isInstanceOf(TransformationException.class)
                .hasMessageContaining("boom");

        EtlJobMetricsSnapshot snapshot = collector.getSnapshot(job.jobId()).orElseThrow();
        assertThat(snapshot.status()).isEqualTo(EtlJobStatus.FAILED);
        assertThat(snapshot.extractedRecords()).isEqualTo(2);
        assertThat(snapshot.errorCount()).isEqualTo(1);
        assertThat(deadLetterQueue.getEntries(job.jobId())).hasSize(1);

        assertThat(listener.statuses())
                .containsExactly(EtlJobStatus.RUNNING, EtlJobStatus.FAILED);
        assertThat(listener.errorCount()).isEqualTo(1);
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

    private static final class RecordingMetricsListener implements EtlMetrics {
        private final List<EtlJobStatus> statuses = new ArrayList<>();
        private int errorCount;

        @Override
        public void onJobStatusChanged(EtlJob job, EtlJobStatus status) {
            statuses.add(status);
        }

        @Override
        public void onExtractStart(EtlJob job) {
            // no-op
        }

        @Override
        public void onExtractComplete(EtlJob job, int extractedRecords, long durationMillis) {
            // no-op
        }

        @Override
        public void onTransformStart(EtlJob job, int inputRecords) {
            // no-op
        }

        @Override
        public void onTransformComplete(EtlJob job, int outputRecords, long durationMillis) {
            // no-op
        }

        @Override
        public void onLoadStart(EtlJob job, int inputRecords) {
            // no-op
        }

        @Override
        public void onLoadComplete(EtlJob job, int loadedRecords, long durationMillis) {
            // no-op
        }

        @Override
        public void onRecordProcessed(EtlJob job, EtlRecord record) {
            // not used in these tests
        }

        @Override
        public void onError(EtlJob job, EtlException exception) {
            errorCount++;
        }

        List<EtlJobStatus> statuses() {
            return statuses;
        }

        int errorCount() {
            return errorCount;
        }
    }

}
