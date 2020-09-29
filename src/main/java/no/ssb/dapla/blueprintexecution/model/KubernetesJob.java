package no.ssb.dapla.blueprintexecution.model;

import io.helidon.common.reactive.Single;

import java.util.concurrent.Executor;

/**
 * Represent the execution of a notebook job
 */
public class KubernetesJob extends AbstractJob {

    private final Executor executor;
    private final Notebook notebook;

    public KubernetesJob(Executor executor, Notebook notebook) {
        this.executor = executor;
        this.notebook = notebook;
    }

    @Override
    protected Single<AbstractJob> startJob() {
        return null;
    }
}
