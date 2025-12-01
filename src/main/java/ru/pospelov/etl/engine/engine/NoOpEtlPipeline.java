package ru.pospelov.etl.engine.engine;

import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.model.EtlJob;

@Component
public class NoOpEtlPipeline implements EtlPipeline {

    private final CancellationToken cancellationToken = new CancellationToken();

    @Override
    public void run(EtlJob job) {
        cancellationToken.checkCancellation();
        System.out.println("NoOp pipeline executed for job: " + job.getJobId());
    }

    @Override
    public void cancel() {
        cancellationToken.cancel();
    }

    @Override
    public boolean isCancelled() {
        return cancellationToken.isCancelled();
    }
}

