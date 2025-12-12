package ru.pospelov.etl.engine;

import org.junit.jupiter.api.Test;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.config.loader.JdbcLoaderConfig;
import ru.pospelov.etl.engine.config.transformer.NoopTransformerConfig;
import ru.pospelov.etl.engine.support.DeadLetterEntry;
import ru.pospelov.etl.engine.pipeline.EtlComponentFactory;
import ru.pospelov.etl.engine.pipeline.EtlPipeline;
import ru.pospelov.etl.engine.pipeline.EtlPipelineFactory;
import ru.pospelov.etl.engine.support.InMemoryDeadLetterQueue;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.exception.TransformationException;
import ru.pospelov.etl.engine.metrics.EtlJobMetricsSnapshot;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EtlErrorHandlingTest {

    @Test
    void nonCriticalTransformationErrorGoesToDeadLetterQueue() {
        InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();
        EtlMetricsCollector metricsCollector = new EtlMetricsCollector();
        EtlComponentFactory componentFactory = mock(EtlComponentFactory.class);

        EtlJob job = createJob("job-non-critical");
        EtlRecord first = new EtlRecord(Instant.now(), "test", 1L);
        EtlRecord second = new EtlRecord(Instant.now(), "test", 2L);

        doAnswer(invocation -> {
            Consumer<Collection<EtlRecord>> consumer = invocation.getArgument(1);
            consumer.accept(List.of(first));
            consumer.accept(List.of(second));
            return null;
        }).when(componentFactory).extract(eq(job), any());

        when(componentFactory.transform(eq(job), any())).thenAnswer(invocation -> {
            Collection<EtlRecord> records = invocation.getArgument(1);
            EtlRecord record = records.iterator().next();
            if (record.getOffset() == 2L) {
                throw new TransformationException(
                        "Synthetic transformation error",
                        job.jobId(),
                        record,
                        EtlErrorSeverity.NON_CRITICAL
                );
            }
            return records;
        });

        List<EtlRecord> loadedRecords = new ArrayList<>();
        doAnswer(invocation -> {
            loadedRecords.addAll(invocation.getArgument(1));
            return null;
        }).when(componentFactory).load(eq(job), any());

        EtlPipeline pipeline = new EtlPipelineFactory(componentFactory, deadLetterQueue, metricsCollector).createStreamingEtlPipeline();

        assertThatThrownBy(() -> pipeline.run(job))
                .isInstanceOf(TransformationException.class)
                .hasMessageContaining("Synthetic transformation error");

        assertThat(loadedRecords).hasSize(1);
        assertThat(loadedRecords.getFirst().getOffset()).isEqualTo(1L);

        List<DeadLetterEntry> entries = deadLetterQueue.getEntries(job.jobId());
        assertThat(entries).hasSize(1);
        assertThat(entries.getFirst().record()).isNotNull();
        assertThat(entries.getFirst().record().getOffset()).isEqualTo(2L);
        assertThat(entries.getFirst().severity()).isEqualTo(EtlErrorSeverity.NON_CRITICAL);

        EtlJobMetricsSnapshot snapshot = metricsCollector.getSnapshot(job.jobId()).orElseThrow();
        assertThat(snapshot.status()).isEqualTo(EtlJobStatus.FAILED);
        assertThat(snapshot.errorCount()).isEqualTo(1);
    }

    @Test
    void criticalLoaderErrorStopsPipeline() {
        InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();
        EtlMetricsCollector metricsCollector = new EtlMetricsCollector();
        EtlComponentFactory componentFactory = mock(EtlComponentFactory.class);

        EtlJob job = createJob("job-critical");
        EtlRecord record = new EtlRecord(Instant.now(), "test", 5L);

        doAnswer(invocation -> {
            Consumer<Collection<EtlRecord>> consumer = invocation.getArgument(1);
            consumer.accept(List.of(record));
            return null;
        }).when(componentFactory).extract(eq(job), any());

        when(componentFactory.transform(eq(job), any()))
                .thenAnswer(invocation -> invocation.<Collection<EtlRecord>>getArgument(1));

        doAnswer(invocation -> {
            throw new LoadingException("forced failure", job.jobId(), record, EtlErrorSeverity.CRITICAL);
        }).when(componentFactory).load(eq(job), any());

        EtlPipeline pipeline = new EtlPipelineFactory(componentFactory, deadLetterQueue, metricsCollector).createStreamingEtlPipeline();

        assertThatThrownBy(() -> pipeline.run(job))
                .isInstanceOf(LoadingException.class)
                .hasMessageContaining("forced failure");

        List<DeadLetterEntry> entries = deadLetterQueue.getEntries(job.jobId());
        assertThat(entries).hasSize(1);
        assertThat(entries.getFirst().record()).isNotNull();

        EtlJobMetricsSnapshot snapshot = metricsCollector.getSnapshot(job.jobId()).orElseThrow();
        assertThat(snapshot.status()).isEqualTo(EtlJobStatus.FAILED);
        assertThat(snapshot.errorCount()).isEqualTo(1);
    }

    private EtlJob createJob(String jobId) {
        JdbcExtractorConfig extractorConfig = new JdbcExtractorConfig(
                "SELECT 1",
                Optional.empty(),
                1,
                Optional.empty(),
                1,
                1_000
        );
        NoopTransformerConfig transformerConfig = new NoopTransformerConfig();
        JdbcLoaderConfig loaderConfig = new JdbcLoaderConfig("target_table", 1_000);
        return new EtlJob(jobId, extractorConfig, transformerConfig, loaderConfig);
    }
}
