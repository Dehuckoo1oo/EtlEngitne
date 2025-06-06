package ru.pospelov.etl.engine.engine;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.steps.extractor.Extractor;
import ru.pospelov.etl.engine.steps.loader.Loader;
import ru.pospelov.etl.engine.steps.transformer.Transformer;

@Component
public class EtlPipelineFactory {

    private final EtlComponentRegistry registry;

    public EtlPipelineFactory(EtlComponentRegistry registry) {
        this.registry = registry;
    }

    public EtlPipeline create(EtlJob job) {
        String extractorType = job.getParam("extractorType").toString();
        String transformerType = job.getParam("transformerType").toString();
        String loaderType = job.getParam("loaderType").toString();

        Extractor extractor = registry.getExtractor(extractorType);
        Transformer transformer = registry.getTransformer(transformerType);
        Loader loader = registry.getLoader(loaderType);

        return j -> {
            var records = extractor.extract(j);
            var transformed = transformer.transform(records, j);
            loader.load(transformed, j);
        };
    }
}