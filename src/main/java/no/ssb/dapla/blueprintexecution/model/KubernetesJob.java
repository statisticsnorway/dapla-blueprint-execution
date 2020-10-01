package no.ssb.dapla.blueprintexecution.model;

import io.fabric8.kubernetes.api.model.batch.Job;
import io.helidon.common.reactive.Single;
import io.helidon.config.Config;
import no.ssb.dapla.blueprintexecution.blueprint.Notebook;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookDetail;
import no.ssb.dapla.blueprintexecution.k8s.K8sExecutionJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Represent the execution of a notebook job
 */
public class KubernetesJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(KubernetesJob.class);
    private static final Random random = new Random();

    private final Executor executor;
    private final NotebookDetail notebook;
    private final Config config;

    public KubernetesJob(Executor executor, NotebookDetail notebook, Config config) {
        this.executor = Objects.requireNonNull(executor);
        this.notebook = Objects.requireNonNull(notebook);
        this.config = Objects.requireNonNull(config);
    }

    public Notebook getNotebook() {
        return notebook;
    }

    public void addPrevious(KubernetesJob job) {
        this.previousNodes.add(job);
    }

    @Override
    protected Single<AbstractJob> startJob() {
        // TODO: Actual execution.
        log.info("Submitting notebook {} (id {}) for execution", notebook.path, notebook.id);
        CompletableFuture<AbstractJob> future = CompletableFuture.supplyAsync(() -> {
            try {
                log.info("Starting execution of the notebook {} (id {})", notebook.path, notebook.id);
                Thread.sleep((random.nextInt(10) + 5) * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Done executing the notebook {} (id {})", notebook.path, notebook.id);
            return this;
        }, executor);
        return Single.create(future);
    }

    Job buildJob() {
        K8sExecutionJob jobCreator = new K8sExecutionJob(config, notebook);
        return jobCreator.buildJob();
    }
}
