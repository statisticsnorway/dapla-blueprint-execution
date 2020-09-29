package no.ssb.dapla.blueprintexecution.model;

import io.helidon.common.reactive.Single;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookDetail;

import java.util.concurrent.Executor;

/**
 * Represent the execution of a notebook job
 */
public class KubernetesJob extends AbstractJob {

    private final Executor executor;
    private final NotebookDetail notebook;

    public KubernetesJob(Executor executor, NotebookDetail notebook) {
        this.executor = executor;
        this.notebook = notebook;
    }

    @Override
    protected Single<AbstractJob> startJob() {
        return null;
    }
}
