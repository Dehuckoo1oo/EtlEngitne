package ru.pospelov.etl.engine.engine;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;

/**
 * Factory for creating ETL pipelines.
 * Uses EtlComponentFactory for type-safe component dispatching.
 */
@Component
public class EtlPipelineFactory {

    private final EtlComponentFactory componentFactory;
    private final DeadLetterQueue deadLetterQueue;
    private final EtlMetricsCollector metricsCollector;

    public EtlPipelineFactory(
            EtlComponentFactory componentFactory,
            DeadLetterQueue deadLetterQueue,
            EtlMetricsCollector metricsCollector
    ) {
        this.componentFactory = componentFactory;
        this.deadLetterQueue = deadLetterQueue;
        this.metricsCollector = metricsCollector;
    }

    /**
     * Creates ETL pipeline from job configuration.
     * No validation needed - EtlJob record is already validated by Bean Validation.
     *
     * @return streaming ETL pipeline
     */
    public EtlPipeline createStreamingEtlPipeline() {
        // No validation needed - EtlJob is type-safe and validated
        // ComponentFactory handles type-safe dispatching using pattern matching
        return new StreamingEtlPipeline(
                componentFactory,
                deadLetterQueue,
                metricsCollector
        );
    }
}
