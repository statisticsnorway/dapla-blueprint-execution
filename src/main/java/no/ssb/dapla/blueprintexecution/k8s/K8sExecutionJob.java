package no.ssb.dapla.blueprintexecution.k8s;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class K8sExecutionJob {

    private final static String POD_NAME = "jupyter-execution";
    private final static String NAMESPACE = "dapla-spark";
    private final static String VOLUME_NAME = "notebooks";
    private final static String MOUNTH_PATH = "/" + VOLUME_NAME;

    private final Job job;
    private String jobLog;
    private PodStatus status;
    private final String sourceNotebookUri;

    private static final Logger LOG = LoggerFactory.getLogger(K8sExecutionJob.class);

    public K8sExecutionJob(String sourceNotebookUri) {
        this.job = buildJob();
        this.sourceNotebookUri = sourceNotebookUri;
    }

    public void createAndRunJOb() {
        try (final KubernetesClient client = new DefaultKubernetesClient()) {
            client.batch().jobs().inNamespace(NAMESPACE).create(job);

            // Get all pods created by the job
            PodList podList = client.pods().inNamespace(NAMESPACE).withLabel("job-name", job.getMetadata().getName()).list();

            // Wait for job to complete
            client.pods().inNamespace(NAMESPACE).withName(podList.getItems().get(0).getMetadata().getName())
                    .waitUntilCondition(pod -> pod.getStatus().getPhase().equals("Succeeded") || pod.getStatus().getPhase().equals("Error"),
                            20, TimeUnit.SECONDS);
            this.jobLog = client.batch().jobs().inNamespace(NAMESPACE).withName(POD_NAME + "-job").getLog();

            this.status = podList.getItems().get(0).getStatus();

        } catch (final KubernetesClientException e) {
            LOG.error("Unable to create job " + e);
        } catch (InterruptedException e) {
            LOG.error("Thread interrupted!", e);
            Thread.currentThread().interrupt();
        } finally {
            try (final KubernetesClient client = new DefaultKubernetesClient()) {
                LOG.info("Job completed with status {} and log:\n {}", status.getPhase(), jobLog);
                client.batch().jobs().inNamespace(NAMESPACE).withName(POD_NAME + "-job").delete();
                LOG.info(POD_NAME + "-job was deleted");
            }
        }
    }

    public PodStatus getJobStatus() {
        try (final KubernetesClient client = new DefaultKubernetesClient()) {
            return client.pods().inNamespace(NAMESPACE).withLabel("job-name", job.getMetadata().getName()).list().getItems().get(0).getStatus();
        }
    }

    public String getJobLog() {
        return jobLog;
    }

    private Job buildJob() {
        return new JobBuilder()
                .withNewMetadata()
                .withName(POD_NAME + "-job")
                .withLabels(Collections.singletonMap("label1", "execute_notebook_dag"))
                .endMetadata()
                .withNewSpec()
                .withNewTemplate()
                .withNewSpec()
                .withContainers(createContainer())
                .withRestartPolicy("Never")
                .withInitContainers(createInitContainer())
                .withRestartPolicy("Never")
                .withVolumes(createVolume())
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
    }

    private Container createContainer() {
        return new ContainerBuilder()
                .withName(POD_NAME)
                .withImage("eu.gcr.io/prod-bip/ssb/dapla/dapla-jupyterlab:master-35989879b39e8fdf06e971c29f2de7678c1180cc")
                .withCommand(String.format("papermill %s /notebook/result.ipynb -k pyspark_k8s", sourceNotebookUri))
                .addNewVolumeMount()
                .withName(VOLUME_NAME)
                .withMountPath(MOUNTH_PATH)
                .endVolumeMount()
                .build();
    }

    private Container createInitContainer() {
        return new ContainerBuilder()
                .withName("copy-notebooks")
                .withImage("busybox")
//                    .withCommand("sh -c echo -n THIS IS WORKING > /notebooks/testfile")
                .addNewVolumeMount()
                .withName(VOLUME_NAME)
                .withMountPath(MOUNTH_PATH)
                .endVolumeMount()
                .build();
    }

    private Volume createVolume() {
        return new VolumeBuilder()
                .withName(VOLUME_NAME)
                .withEmptyDir(new EmptyDirVolumeSourceBuilder().build())
                .build();
    }

}
