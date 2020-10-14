package no.ssb.dapla.blueprintexecution.k8s;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.fabric8.kubernetes.api.model.batch.Job;
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
import java.util.Objects;
import java.util.Random;

public class K8sJobTemplate {

    private static final Logger LOG = LoggerFactory.getLogger(K8sJobTemplate.class);
    private static final URL template = K8sJobTemplate.class.getResource("job.template.yaml");
    private final Config config;
    private final NotebookDetail notebook;
    private final Random random;

    public K8sJobTemplate(Config config, NotebookDetail notebook) {
        this(config, notebook, new SecureRandom());
    }

    public K8sJobTemplate(Config config, NotebookDetail notebook, Random random) {
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

}
