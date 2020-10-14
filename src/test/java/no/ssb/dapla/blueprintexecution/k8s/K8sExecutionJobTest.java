package no.ssb.dapla.blueprintexecution.k8s;

import io.helidon.config.Config;
import io.helidon.config.ConfigSources;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookDetail;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class K8sExecutionJobTest {

    private static final Random RANDOM = new Random(1234);

    @Test
    void testTemplating() throws IOException {

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

        K8sExecutionJob k8sExecutionJob = new K8sExecutionJob(config, detail, RANDOM);

        byte[] job = k8sExecutionJob.interpolateTemplate().readAllBytes();
        byte[] expected = getClass().getResourceAsStream("expected_job.yaml").readAllBytes();

        assertThat(new String(job)).isEqualTo(new String(expected));

    }
}