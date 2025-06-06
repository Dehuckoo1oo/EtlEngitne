package ru.pospelov.etl.engine.runner;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.engine.EtlPipeline;
import ru.pospelov.etl.engine.model.EtlJob;

import java.util.Map;

@Component
public class EtlJobRunner implements CommandLineRunner {
    private final EtlPipeline pipeline;
    public EtlJobRunner(EtlPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public void run(String... args) {
        EtlJob job = new EtlJob(
                "kafka-to-sql",
                null,
                "target_table",
                Map.of("extractorType", "kafka", "loaderType", "sql", "transformerType", "noop")
        );
        pipeline.run(job);
    }
}
