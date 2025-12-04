package ru.pospelov.etl.engine;

import org.junit.jupiter.api.Test;
import ru.pospelov.etl.engine.engine.DeadLetterQueue;
import ru.pospelov.etl.engine.engine.EtlComponentRegistry;
import ru.pospelov.etl.engine.engine.EtlPipeline;
import ru.pospelov.etl.engine.engine.EtlPipelineFactory;
import ru.pospelov.etl.engine.engine.InMemoryDeadLetterQueue;
import ru.pospelov.etl.engine.validation.JobValidator;
import ru.pospelov.etl.engine.validation.ValidationResult;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.EtlException;
import ru.pospelov.etl.engine.exception.TransformationException;
import ru.pospelov.etl.engine.metrics.EtlJobMetricsSnapshot;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetrics;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EtlPipelineMetricsTest {

    @Test
    void successfulJobProducesMetricsAndStatuses() {
        EtlMetricsCollector collector = new EtlMetricsCollector();
        RecordingMetricsListener listener = new RecordingMetricsListener();
        collector.registerListener(listener);

        EtlPipeline pipeline = createPipeline(
                new InMemoryDeadLetterQueue(),
                new FixedExtractor(),
                new PassThroughTransformer(),
                new RecordingLoader(),
                collector
        );

        EtlJob job = createJob("metrics-success", "fixed-extractor", "pass-transformer", "recording-loader");
        pipeline.run(job);

        EtlJobMetricsSnapshot snapshot = collector.getSnapshot(job.getJobId()).orElseThrow();
        assertThat(snapshot.status()).isEqualTo(EtlJobStatus.COMPLETED);
        assertThat(snapshot.extractedRecords()).isEqualTo(2);
        assertThat(snapshot.transformedRecords()).isEqualTo(2);
        assertThat(snapshot.loadedRecords()).isEqualTo(2);
        // processedRecords больше не учитывается (убрали onRecordProcessed для производительности)
        // assertThat(snapshot.processedRecords()).isEqualTo(2);
        assertThat(snapshot.errorCount()).isZero();

        assertThat(listener.statuses())
                .containsExactly(
                        EtlJobStatus.RUNNING,
                        EtlJobStatus.EXTRACTING,
                        EtlJobStatus.TRANSFORMING,
                        EtlJobStatus.LOADING,
                        EtlJobStatus.COMPLETED
                );
        // processedRecords больше не отслеживается
        // assertThat(listener.processedRecords()).isEqualTo(2);
        assertThat(listener.errorCount()).isZero();
    }

    @Test
    void failingTransformerMarksStatusFailedAndCountsError() {
        EtlMetricsCollector collector = new EtlMetricsCollector();
        RecordingMetricsListener listener = new RecordingMetricsListener();
        collector.registerListener(listener);

        EtlPipeline pipeline = createPipeline(
                new InMemoryDeadLetterQueue(),
                new FixedExtractor(),
                new FailingTransformer(),
                new RecordingLoader(),
                collector
        );

        EtlJob job = createJob("metrics-failure", "fixed-extractor", "failing-transformer", "recording-loader");

        assertThatThrownBy(() -> pipeline.run(job))
                .isInstanceOf(TransformationException.class)
                .hasMessageContaining("boom");

        EtlJobMetricsSnapshot snapshot = collector.getSnapshot(job.getJobId()).orElseThrow();
        assertThat(snapshot.status()).isEqualTo(EtlJobStatus.FAILED);
        // Записи были извлечены до того как трансформация упала
        assertThat(snapshot.extractedRecords()).isEqualTo(2);
        assertThat(snapshot.errorCount()).isEqualTo(1);

        assertThat(listener.statuses())
                .containsExactly(
                        EtlJobStatus.RUNNING,
                        EtlJobStatus.EXTRACTING,
                        EtlJobStatus.TRANSFORMING,
                        EtlJobStatus.FAILED
                );
        assertThat(listener.errorCount()).isEqualTo(1);
    }

    private EtlPipeline createPipeline(
            DeadLetterQueue deadLetterQueue,
            Extractor extractor,
            Transformer transformer,
            Loader loader,
            EtlMetricsCollector collector
    ) {
        EtlComponentRegistry registry = new EtlComponentRegistry(
                List.of(extractor),
                List.of(transformer),
                List.of(loader)
        );
        JobValidator jobValidator = job -> new ValidationResult();
        EtlPipelineFactory factory = new EtlPipelineFactory(registry, deadLetterQueue, collector, jobValidator);
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
            EtlRecord second = new EtlRecord(Instant.now(), "test", 2L);
            batchConsumer.accept(List.of(first, second));
        }

        @Override
        public String getType() {
            return "fixed-extractor";
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

    private static class FailingTransformer implements Transformer {
        @Override
        public Collection<EtlRecord> transform(Collection<EtlRecord> etlRecords, EtlJob job) {
            throw new TransformationException("boom", job.getJobId(), null, EtlErrorSeverity.CRITICAL);
        }

        @Override
        public String getType() {
            return "failing-transformer";
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
    }

    private static final class RecordingMetricsListener implements EtlMetrics {
        private final List<EtlJobStatus> statuses = new ArrayList<>();
        private int processedRecords;
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
            processedRecords++;
        }

        @Override
        public void onError(EtlJob job, EtlException exception) {
            errorCount++;
        }

        List<EtlJobStatus> statuses() {
            return statuses;
        }

        int processedRecords() {
            return processedRecords;
        }

        int errorCount() {
            return errorCount;
        }
    }

}

