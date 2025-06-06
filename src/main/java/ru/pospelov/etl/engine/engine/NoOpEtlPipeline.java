package ru.pospelov.etl.engine.engine;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlJob;

@Component
public class NoOpEtlPipeline implements EtlPipeline {

    @Override
    public void run(EtlJob job) {
        System.out.println("NoOp pipeline executed for job: " + job.getJobId());
    }
}

