package no.ssb.dapla.blueprintexecution.k8s;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.helidon.config.Config;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookDetail;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class K8sExecutionJob {

    private static final Logger LOG = LoggerFactory.getLogger(K8sExecutionJob.class);
    @Deprecated
    private final Job job;
    @Deprecated
    private final String sourceNotebookUri;
    @Deprecated
    private String jobLog;
    @Deprecated
    private PodStatus status;

    private Config config;
    private NotebookDetail notebook;

    @Deprecated
    public K8sExecutionJob(String sourceNotebookUri) {
        this.job = buildJob();
        this.sourceNotebookUri = sourceNotebookUri;
    }

    public K8sExecutionJob(Config config, NotebookDetail notebook) {
        this.config = Objects.requireNonNull(config);
        this.notebook = Objects.requireNonNull(notebook);

        this.sourceNotebookUri = "";
        this.job = null;
    }

    public void test() throws IOException {
        Writer writer = new OutputStreamWriter(System.out);
        MustacheFactory mf = new DefaultMustacheFactory();
        InputStreamReader reader = new InputStreamReader(getClass().getResourceAsStream("job.template.yaml"));
        Mustache mustache = mf.compile(reader, "job.template.yaml");
        mustache.execute(writer, this);
        writer.flush();
    }

    private String getNamespace() {
        return this.config.get("k8s.namespace").asString().get();
    }

    private String getVolumeName() {
        return this.config.get("k8s.volume-name").asString().get();
    }

    private String getPodPrefix() {
        String prefix = this.config.get("k8s.pod-prefix").asString().get();
        return prefix + "-" + this.notebook.id.substring(0, 7);
    }

    private String getMountPath() {
        return Path.of(this.config.get("k8s.mount-path").asString().get()).normalize().toString();
    }

    private String getContainerImage() {
        return this.config.get("k8s.container.image").asString().get();
    }

    private String getContainerMemoryLimit() {
        return this.config.get("k8s.container.memory-limit").asString().get();
    }

    private String getContainerMemoryRequest() {
        return this.config.get("k8s.container.memory-request").asString().get();
    }

    private String getNotebookUri() {
        return URI.create(
                this.config.get("blueprint.url").asString().get() + "/" +
                        notebook.fetchUrl
        ).normalize().toASCIIString();
    }

    private String getNotebookPath() {
        return Path.of(
                getMountPath(),
                "notebook.ipynb"
        ).normalize().toString();
    }

    private String getOutputPath() {
        return Path.of(
                getMountPath(),
                "result.ipynb"
        ).normalize().toString();
    }

    @Deprecated
    public void createAndRunJOb() {
        try (final KubernetesClient client = new DefaultKubernetesClient()) {
            client.batch().jobs().inNamespace(getNamespace()).create(job);

            // Get all pods created by the job
            PodList podList = client.pods().inNamespace(getNamespace()).withLabel("job-name", job.getMetadata().getName()).list();

            // Wait for job to complete
            client.pods().inNamespace(getNamespace()).withName(podList.getItems().get(0).getMetadata().getName())
                    .waitUntilCondition(pod -> pod.getStatus().getPhase().equals("Succeeded") || pod.getStatus().getPhase().equals("Error"),
                            20, TimeUnit.SECONDS);
            this.jobLog = client.batch().jobs().inNamespace(getNamespace()).withName(getPodPrefix() + "-job").getLog();
            this.status = podList.getItems().get(0).getStatus();

        } catch (final KubernetesClientException e) {
            LOG.error("Unable to create job " + e);
        } catch (InterruptedException e) {
            LOG.error("Thread interrupted!", e);
            Thread.currentThread().interrupt();
        } finally {
            try (final KubernetesClient client = new DefaultKubernetesClient()) {
                LOG.info("Job completed with status {} and log:\n {}", status.getPhase(), jobLog);
                client.batch().jobs().inNamespace(getNamespace()).withName(getPodPrefix() + "-job").delete();
                LOG.info(getPodPrefix() + "-job was deleted");
            }
        }
    }

    @Deprecated
    public PodStatus getJobStatus() {
        try (final KubernetesClient client = new DefaultKubernetesClient()) {
            return client.pods().inNamespace(getNamespace()).withLabel("job-name", job.getMetadata().getName()).list().getItems().get(0).getStatus();
        }
    }

    @Deprecated
    public String getJobLog() {
        return jobLog;
    }

    public Job buildJob() {
        return new JobBuilder()
                .withNewMetadata()
//                .withGenerateName(getPodPrefix())
                .withName(getPodPrefix() + RandomStringUtils.random(5, true, true).toLowerCase() + "-job") // add random string
                .withLabels(Collections.singletonMap("label1", "execute_notebook_dag"))
                .withNamespace(getNamespace())
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

    public Container createContainer() {
        return new ContainerBuilder()
                .withName(getPodPrefix() + "-cont")
                .withImage(getContainerImage())
                .withResources(createResourceRequirements())
                .withCommand("papermill", getNotebookPath(), getOutputPath(), "-k", "pyspark_k8s")
                .addNewVolumeMount()
                .withName(getVolumeName())
                .withMountPath(getMountPath())
                .endVolumeMount()
                .build();
    }

    public ResourceRequirements createResourceRequirements() {
        return new ResourceRequirementsBuilder()
                .withLimits(Map.of("memory", Quantity.parse(getContainerMemoryLimit())))
                .withRequests(Map.of("memory", Quantity.parse(getContainerMemoryRequest())))
                .build();
    }

    public Container createInitContainer() {
        return new ContainerBuilder()
                .withName("copy-notebooks")
                .withImage(getContainerImage())
                .withCommand("curl", "-X", "GET", getNotebookUri(), "-H", "accept: application/x-ipynb+json", "-o", getNotebookPath())
                .addNewVolumeMount()
                .withName(getVolumeName())
                .withMountPath(getMountPath())
                .endVolumeMount()
                .build();
    }

    public Volume createVolume() {
        return new VolumeBuilder()
                .withName(getVolumeName())
                .withEmptyDir(new EmptyDirVolumeSourceBuilder().build())
                .build();
    }

}
