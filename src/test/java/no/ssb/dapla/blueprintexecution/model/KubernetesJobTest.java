package no.ssb.dapla.blueprintexecution.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.helidon.config.Config;
import io.helidon.config.ConfigSources;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookDetail;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;

import static io.fabric8.kubernetes.client.internal.SerializationUtils.dumpWithoutRuntimeStateAsYaml;
import static org.assertj.core.api.Assertions.assertThat;

class KubernetesJobTest {

    static ObjectMapper yamlMapper;

    @BeforeAll
    static void beforeAll() {
        yamlMapper = new ObjectMapper(new YAMLFactory());
        yamlMapper = yamlMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    }

    @Test
    void testJobYaml() throws IOException {
        NotebookDetail detail = new NotebookDetail();
        detail.id = "757b9c7c9e1351d7811a68466026ac8622169ad3";
        detail.commitId = "785257f87f37a40fe1d7641e1b5a95017e1b2b47";
        detail.path = "blueprint/tests/2.ipynb";
        detail.fetchUrl = "/api/v1/repositories/foo/bar";

        Config config = Config.create(ConfigSources.create(Map.of(
                "k8s.namespace", "dummy-namespace",
                "k8s.volume-name", "dummy-volume-name",
                "k8s.pod-prefix", "dummy-pod-prefix",
                "k8s.mount-path", "/dummy-mount-path//",
                "k8s.container.memory-limit", "1234Mi",
                "k8s.container.memory-request", "4321Mi",
                "k8s.container.image", "dummy:image",
                "blueprint.url", "https://example.com//"
        )));

        KubernetesJob kubernetesJob = new KubernetesJob(Executors.newSingleThreadExecutor(), detail, config, null);
        Job job = kubernetesJob.buildJob();

        // Remove random part of job name
        String[] jobNameArray = job.getMetadata().getName().split("-");
        jobNameArray[3] = jobNameArray[3].substring(0, 7);
        job.getMetadata().setName(StringUtils.join(jobNameArray, "-"));

        Job expectedJob = yamlMapper.readValue(
                getClass().getResource("expected_job.yaml"),
                Job.class
        );

        assertThat(job).isEqualTo(expectedJob);

        // Serialize/Deserialize to get comparable objects.
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        yamlMapper.writeValue(outputStream, job);

        JsonNode jobNode = yamlMapper.readValue(new ByteArrayInputStream(outputStream.toByteArray()),
                JsonNode.class);
        JsonNode expectedJobNode = yamlMapper.readValue(
                getClass().getResource("expected_job.yaml"),
                JsonNode.class
        );

        System.out.println(dumpWithoutRuntimeStateAsYaml(job));

        assertThat(jobNode).usingRecursiveComparison().isEqualTo(expectedJobNode);
    }
}