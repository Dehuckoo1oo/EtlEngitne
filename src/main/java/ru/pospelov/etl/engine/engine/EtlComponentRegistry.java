package ru.pospelov.etl.engine.engine;


import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.steps.extractor.Extractor;
import ru.pospelov.etl.engine.steps.loader.Loader;
import ru.pospelov.etl.engine.steps.transformer.Transformer;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class EtlComponentRegistry {

    private final Map<String, Extractor> extractorMap;
    private final Map<String, Transformer> transformerMap;
    private final Map<String, Loader> loaderMap;

    public EtlComponentRegistry(
            List<Extractor> extractors,
            List<Transformer> transformers,
            List<Loader> loaders) {
        this.extractorMap = extractors.stream().collect(Collectors.toMap(Extractor::getType, Function.identity()));
        this.transformerMap = transformers.stream().collect(Collectors.toMap(Transformer::getType, Function.identity()));
        this.loaderMap = loaders.stream().collect(Collectors.toMap(Loader::getType, Function.identity()));
    }

    public Extractor getExtractor(String type) {
        return extractorMap.get(type);
    }

    public Transformer getTransformer(String type) {
        return transformerMap.get(type);
    }

    public Loader getLoader(String type) {
        return loaderMap.get(type);
    }
}

