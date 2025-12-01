package ru.pospelov.etl.engine;

import org.junit.jupiter.api.Test;
import ru.pospelov.etl.engine.engine.DeadLetterEntry;
import ru.pospelov.etl.engine.engine.DeadLetterQueue;
import ru.pospelov.etl.engine.engine.EtlComponentRegistry;
import ru.pospelov.etl.engine.engine.EtlPipeline;
import ru.pospelov.etl.engine.engine.EtlPipelineFactory;
import ru.pospelov.etl.engine.engine.InMemoryDeadLetterQueue;
import ru.pospelov.etl.engine.validation.DefaultJobValidator;
import ru.pospelov.etl.engine.validation.ExtractorValidator;
import ru.pospelov.etl.engine.validation.LoaderValidator;
import ru.pospelov.etl.engine.validation.TransformerValidator;
import ru.pospelov.etl.engine.exception.EtlErrorSeverity;
import ru.pospelov.etl.engine.exception.LoadingException;
import ru.pospelov.etl.engine.exception.TransformationException;
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

class EtlErrorHandlingTest {

    @Test
    void nonCriticalTransformationErrorGoesToDeadLetterQueue() {
        InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();
        RecordingLoader loader = new RecordingLoader();
        EtlPipeline pipeline = createPipeline(
                deadLetterQueue,
                new FixedExtractor(),
                new SelectiveFailingTransformer(2L),
                loader
        );
        EtlJob job = createJob("job-non-critical", "fixed-extractor", "failing-transformer", "recording-loader");

        pipeline.run(job);

        List<EtlRecord> loaded = loader.getLoadedRecords();
        assertThat(loaded).hasSize(1);
        assertThat(loaded.get(0).getOffset()).isEqualTo(1L);

        List<DeadLetterEntry> entries = deadLetterQueue.getEntries("job-non-critical");
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).record()).isNotNull();
        assertThat(entries.get(0).record().getOffset()).isEqualTo(2L);
        assertThat(entries.get(0).severity()).isEqualTo(EtlErrorSeverity.NON_CRITICAL);
    }

    @Test
    void criticalLoaderErrorStopsPipeline() {
        DeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();
        EtlPipeline pipeline = createPipeline(
                deadLetterQueue,
                new FixedExtractor(),
                new PassthroughTransformer(),
                new AlwaysFailingLoader()
        );
        EtlJob job = createJob("job-critical", "fixed-extractor", "passthrough-transformer", "failing-loader");

        assertThatThrownBy(() -> pipeline.run(job))
                .isInstanceOf(LoadingException.class)
                .hasMessageContaining("forced failure");

        List<DeadLetterEntry> entries = deadLetterQueue.getEntries("job-critical");
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).record()).isNotNull();
    }

    private EtlPipeline createPipeline(
            DeadLetterQueue deadLetterQueue,
            Extractor extractor,
            Transformer transformer,
            Loader loader
    ) {
        EtlComponentRegistry registry = new EtlComponentRegistry(
                List.of(extractor),
                List.of(transformer),
                List.of(loader)
        );
        EtlMetricsCollector metricsCollector = new EtlMetricsCollector();
        DefaultJobValidator jobValidator = new DefaultJobValidator(
                registry,
                new ExtractorValidator(),
                new TransformerValidator(),
                new LoaderValidator()
        );
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
            second.put("value", "bad");
            batchConsumer.accept(List.of(first, second));
        }

        @Override
        public String getType() {
            return "fixed-extractor";
        }
    }

    private static class SelectiveFailingTransformer implements Transformer {
        private final long failingOffset;

        private SelectiveFailingTransformer(long failingOffset) {
            this.failingOffset = failingOffset;
        }

        @Override
        public Collection<EtlRecord> transform(Collection<EtlRecord> etlRecords, EtlJob job) {
            EtlRecord record = etlRecords.iterator().next();
            if (record.getOffset() == failingOffset) {
                throw new TransformationException(
                        "Synthetic transformation error",
                        job.getJobId(),
                        record,
                        EtlErrorSeverity.NON_CRITICAL
                );
            }
            return etlRecords;
        }

        @Override
        public String getType() {
            return "failing-transformer";
        }
    }

    private static class PassthroughTransformer implements Transformer {
        @Override
        public Collection<EtlRecord> transform(Collection<EtlRecord> etlRecords, EtlJob job) {
            return etlRecords;
        }

        @Override
        public String getType() {
            return "passthrough-transformer";
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

    private static class AlwaysFailingLoader implements Loader {
        @Override
        public void load(Collection<EtlRecord> etlRecords, EtlJob job) {
            EtlRecord record = etlRecords.iterator().next();
            throw new LoadingException("forced failure", job.getJobId(), record, EtlErrorSeverity.CRITICAL);
        }

        @Override
        public String getType() {
            return "failing-loader";
        }
    }
}

