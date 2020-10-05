package no.ssb.dapla.blueprintexecution.model;

import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Represent the execution of a notebook job
 */
public class KubernetesJob extends AbstractJob {

    private static final Logger log = LoggerFactory.getLogger(KubernetesJob.class);
    private static final Random random = new Random();

    private final Executor executor;
    private final NotebookDetail notebook;
    private final KubernetesClient client;
    private final Config config;

    public KubernetesJob(Executor executor, NotebookDetail notebook, Config config, KubernetesClient client) {
        this.executor = Objects.requireNonNull(executor);
        this.notebook = Objects.requireNonNull(notebook);
        this.config = Objects.requireNonNull(config);
        this.client = client;
    }

    public Notebook getNotebook() {
        return notebook;
    }

    public void addPrevious(KubernetesJob job) {
        this.previousNodes.add(job);
    }

    @Override
    protected Single<AbstractJob> startJob() {
        var future = CompletableFuture.supplyAsync(() -> {
            var jobToCreate = buildJob();

            try {

                log.info("Submitting job {} in namespace {}",
                        jobToCreate.getMetadata().getName(),
                        jobToCreate.getMetadata().getNamespace()
                );

                var job = client.batch().jobs().inNamespace(jobToCreate.getMetadata().getNamespace())
                        .createOrReplace(jobToCreate);

                var jobName = job.getMetadata().getName();
                var jobUid = job.getMetadata().getUid();
                var jobNamespace = jobToCreate.getMetadata().getNamespace();
                var jobClusterName = job.getMetadata().getClusterName();

                // Jobs create pods with a job-name label.
                var podNames = client.pods().inNamespace(job.getMetadata().getNamespace())
                        .withLabel("job-name", job.getMetadata().getName())
                        .list().getItems().stream().map(pod -> pod.getMetadata().getName())
                        .collect(Collectors.toList());

                this.setRunning();

                log.info("Created job {}({}) in cluster {}\n{}", jobName, jobUid,
                        jobClusterName, podNames);


                client.pods().inNamespace(jobNamespace).withName(podNames.get(0))
                        .waitUntilCondition(pod -> {
                            return pod.getStatus().getPhase().equals("Succeeded") || pod.getStatus().getPhase().equals("Error");
                        }, 10, TimeUnit.HOURS);

                log.info("Done executing job ({}) {} in cluster {}\n{}", jobName, jobUid,
                        jobClusterName, podNames);

            } catch (InterruptedException e) {
                log.error("Thread interrupted!", e);
                Thread.currentThread().interrupt();
            } catch (Exception ex) {
                log.error("Failed to execute job", ex);
                throw ex;
            }

            return this;
        }, executor);

        return Single.create(future)
                .onComplete(this::setDone)
                .onCancel(this::setCancelled)
                .onError(this::setFailed)
                .map(kubernetesJob -> this);
    }

    Job buildJob() {
        K8sExecutionJob jobCreator = new K8sExecutionJob(config, notebook);
        return jobCreator.buildJob();
    }
}
