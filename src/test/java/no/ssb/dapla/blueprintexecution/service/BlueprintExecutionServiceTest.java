package no.ssb.dapla.blueprintexecution.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.extended.run.EditableRunConfig;
import io.fabric8.kubernetes.client.extended.run.RunConfigBuilder;
import io.fabric8.kubernetes.client.extended.run.RunOperations;
import io.fabric8.kubernetes.client.internal.SerializationUtils;
import io.helidon.common.http.Http;
import io.helidon.common.reactive.Single;
import io.helidon.config.Config;
import io.helidon.webclient.WebClient;
import io.helidon.webclient.WebClientResponse;
import io.helidon.webserver.WebServer;
import no.ssb.dapla.blueprintexecution.BlueprintExecutionApplication;
import no.ssb.dapla.blueprintexecution.HelidonConfigExtension;
import no.ssb.dapla.blueprintexecution.model.Edge;
import no.ssb.dapla.blueprintexecution.model.Notebook;
import okhttp3.Response;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static no.ssb.dapla.blueprintexecution.WebClientResponseAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(HelidonConfigExtension.class)
public class BlueprintExecutionServiceTest {

    private static WebServer server;
    private static WebClient client;

    @BeforeAll
    static void beforeAll(Config config) throws InterruptedException, ExecutionException, TimeoutException {
        server = new BlueprintExecutionApplication(config).get(WebServer.class);
        server.start().get(10, TimeUnit.SECONDS);
        client = WebClient.builder().baseUri("http://localhost:" + server.port()).build();
    }

    @Test
    public void thatTestEndpointWorks() throws ExecutionException, InterruptedException {

        Single<WebClientResponse> response = client.get().path("/").submit();
        assertThat(response)
                .succeedsWithin(1, TimeUnit.SECONDS);
        assertThat(response.await())
                .hasStatus(Http.Status.OK_200);
        assertThat(response.get().content().as(String.class).get())
                .isEqualTo("Server is up and running");
    }

    @Test
    @Ignore
    void testKubernetesJob() throws Exception {
        String podName = "jupyter-execution";
        String namespace = "dapla-spark";

        try (final KubernetesClient client = new DefaultKubernetesClient()) {

            Container jupyterLabContainer = new ContainerBuilder()
                    .withName(podName)
                    .withImage("eu.gcr.io/prod-bip/ssb/dapla/dapla-jupyterlab:master-35989879b39e8fdf06e971c29f2de7678c1180cc")
//                    .withCommand("papermill /notebook/test.ipynb /notebook/test_result.ipynb -k pyspark_k8s")
                    .withCommand("ls")
                    .addNewVolumeMount()
                    .withName("notebooks")
                    .withMountPath("/notebooks")
                    .endVolumeMount()
                    .build();

            Container initContainer = new ContainerBuilder()
                    .withName("copy-notebooks")
                    .withImage("busybox")
//                    .withCommand("sh -c echo -n THIS IS WORKING > /notebooks/testfile")
                    .addNewVolumeMount()
                    .withName("notebooks")
                    .withMountPath("/notebooks")
                    .endVolumeMount()
                    .build();

            Volume volume = new VolumeBuilder()
                    .withName("notebooks")
                    .withEmptyDir(new EmptyDirVolumeSourceBuilder().build())
                    .build();

            final Job job = new JobBuilder()
                    .withNewMetadata()
                    .withName(podName + "-job")
                    .withLabels(Collections.singletonMap("label1", "execute_notebook_dag"))
                    .endMetadata()
                    .withNewSpec()
                    .withNewTemplate()
                    .withNewSpec()
                    .withContainers(jupyterLabContainer)
                    .withRestartPolicy("Never")
                    .withInitContainers(initContainer)
                    .withRestartPolicy("Never")
                    .withVolumes(volume)
                    .endSpec()
                    .endTemplate()
                    .endSpec()
                    .build();

            System.out.println(SerializationUtils.dumpAsYaml(job));

            client.batch().jobs().inNamespace(namespace).create(job);
            // Get All pods created by the job
            PodList podList = client.pods().inNamespace(namespace).withLabel("job-name", job.getMetadata().getName()).list();
            // Wait for pod to complete
            client.pods().inNamespace(namespace).withName(podList.getItems().get(0).getMetadata().getName())
                    .waitUntilCondition(pod -> pod.getStatus().getPhase().equals("Succeeded") || pod.getStatus().getPhase().equals("Error"),
                            5, TimeUnit.SECONDS);

            // Print Job's log
            String joblog = client.batch().jobs().inNamespace(namespace).withName(podName + "-job").getLog();
            System.out.println(joblog);

        } catch (final KubernetesClientException e) {
            System.out.println("Unable to create job " + e);
        } catch (InterruptedException interruptedException) {
            System.out.println("Thread interrupted!");
            Thread.currentThread().interrupt();
        } catch (IllegalArgumentException e) {
            System.out.println("Illegal argument " + e);
        } finally {
            try (final KubernetesClient client = new DefaultKubernetesClient()) {
                client.batch().jobs().inNamespace(namespace).withName(podName + "-job").delete();
                System.out.println("job deleted");
            }
        }
    }

    @Test
    @Ignore
    void testKubernetesClient() throws Exception {
        String podName = "jupyter-execution";
        String namespace = "dapla-spark";

        try (final KubernetesClient client = new DefaultKubernetesClient()) {

            RunOperations run = client.run();
            EditableRunConfig runConfig = new RunConfigBuilder()
                    .addToLimits("memory", new Quantity("2", "Gi"))
                    .withName(podName)
                    .withImage("eu.gcr.io/prod-bip/ssb/dapla/dapla-jupyterlab:master-35989879b39e8fdf06e971c29f2de7678c1180cc")
                    .build();
            Pod pod = run
                    .inNamespace(namespace)
                    .withRunConfig(runConfig)
                    .done();


            Pod pod1 = client.pods().inNamespace(namespace).withName(podName).get();
            System.out.println(pod1.getStatus().getMessage());
            // TODO wait for pod to be up and running - get pod?

            // TODO copy notebooks to container
            // TODO execute papermill
            client.pods().inNamespace(namespace).withName(podName)
                    .readingInput(System.in)
                    .writingOutput(System.out)
                    .writingError(System.err)
                    .usingListener(new SimpleListener())
                    .exec("papermill");

            Thread.sleep(5 * 1000);

            // TODO delete pod after test
            client.pods().inNamespace(namespace).withName(podName).delete();
        }

    }

    private static class SimpleListener implements ExecListener {

        @Override
        public void onOpen(Response response) {
            System.out.println("The shell will remain open for 5 seconds.");
        }

        @Override
        public void onFailure(Throwable t, Response response) {
            System.err.println("shell barfed");
        }

        @Override
        public void onClose(int code, String reason) {
            System.out.println("The shell will now close.");
        }
    }
}