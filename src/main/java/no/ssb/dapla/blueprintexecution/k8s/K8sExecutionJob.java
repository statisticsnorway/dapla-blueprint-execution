package no.ssb.dapla.blueprintexecution.k8s;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.helidon.config.Config;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookDetail;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public class K8sExecutionJob {

    private static final Logger LOG = LoggerFactory.getLogger(K8sExecutionJob.class);
    private static final URL template = K8sExecutionJob.class.getResource("job.template.yaml");
    private final Config config;
    private final NotebookDetail notebook;
    private final Random random;

    public K8sExecutionJob(Config config, NotebookDetail notebook) {
        this(config, notebook, new SecureRandom());
    }

    public K8sExecutionJob(Config config, NotebookDetail notebook, Random random) {
        this.config = Objects.requireNonNull(config);
        this.notebook = Objects.requireNonNull(notebook);
        this.random = Objects.requireNonNull(random);
    }

    public ByteArrayInputStream interpolateTemplate() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        MustacheFactory mf = new DefaultMustacheFactory();
        try (
                var reader = new InputStreamReader(template.openStream());
                var writer = new OutputStreamWriter(output);
        ) {
            Mustache mustache = mf.compile(reader, "job.template.yaml");
            mustache.execute(writer, this);
            writer.flush();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Job template: \n{}", new String(output.toByteArray()));
            }
            return new ByteArrayInputStream(output.toByteArray());
        }
    }

    public Job getJob() throws IOException {
        return Serialization.unmarshal(interpolateTemplate(), Job.class);
    }

    public String getNamespace() {
        return this.config.get("k8s.namespace").asString().get();
    }

    public String getVolumeName() {
        return this.config.get("k8s.volume-name").asString().get();
    }

    public String getPodPrefix() {
        String prefix = this.config.get("k8s.pod-prefix").asString().get();
        return prefix + "-" + this.notebook.id.substring(0, 7) + "-" + getRandom();
    }

    public String getRandom() {
        return RandomStringUtils.random(5, 0, 0, true, true, null, random)
                .toLowerCase();
    }

    public String getMountPath() {
        return Path.of(this.config.get("k8s.mount-path").asString().get()).normalize().toString();
    }

    public String getContainerImage() {
        return this.config.get("k8s.container.image").asString().get();
    }

    public String getContainerMemoryLimit() {
        return this.config.get("k8s.container.memory-limit").asString().get();
    }

    public String getContainerMemoryRequest() {
        return this.config.get("k8s.container.memory-request").asString().get();
    }

    public String getNotebookUri() {
        return URI.create(
                this.config.get("blueprint.url").asString().get() + "/" +
                        notebook.fetchUrl
        ).normalize().toASCIIString();
    }

    public String getNotebookPath() {
        return Path.of(
                getMountPath(),
                "notebook.ipynb"
        ).normalize().toString();
    }

    public String getOutputPath() {
        return Path.of(
                getMountPath(),
                "result.ipynb"
        ).normalize().toString();
    }

    public Job buildJob() {
        return new JobBuilder()
                .withNewMetadata()
//                .withGenerateName(getPodPrefix())
                .withName(getPodPrefix()) // add random string
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
