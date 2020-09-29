package no.ssb.dapla.blueprintexecution.model;

import io.fabric8.kubernetes.api.model.batch.Job;
import io.helidon.common.reactive.Single;
import io.helidon.config.Config;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookDetail;
import no.ssb.dapla.blueprintexecution.k8s.K8sExecutionJob;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Represent the execution of a notebook job
 */
public class KubernetesJob extends AbstractJob {

    private final Executor executor;
    private final NotebookDetail notebook;
    private final Config config;

    public KubernetesJob(Executor executor, NotebookDetail notebook, Config config) {
        this.executor = Objects.requireNonNull(executor);
        this.notebook = Objects.requireNonNull(notebook);
        this.config = Objects.requireNonNull(config);
    }

    @Override
    protected Single<AbstractJob> startJob() {
        return null;
    }

    Job buildJob() {
        K8sExecutionJob jobCreator = new K8sExecutionJob(config, notebook);
        return jobCreator.buildJob();
    }
}
