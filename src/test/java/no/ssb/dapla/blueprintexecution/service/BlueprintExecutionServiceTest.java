package no.ssb.dapla.blueprintexecution.service;

import io.helidon.common.http.Http;
import io.helidon.common.reactive.Single;
import io.helidon.config.Config;
import io.helidon.webclient.WebClient;
import io.helidon.webclient.WebClientResponse;
import io.helidon.webserver.WebServer;
import no.ssb.dapla.blueprintexecution.BlueprintExecutionApplication;
import no.ssb.dapla.blueprintexecution.HelidonConfigExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
}