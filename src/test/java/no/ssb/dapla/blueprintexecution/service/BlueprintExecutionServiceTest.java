package no.ssb.dapla.blueprintexecution.service;

import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(IntegrationTestExtension.class)
public class BlueprintExecutionServiceTest {

    @Inject
    TestClient client;

    @Test
    public void thatTestEndpointWorks() {
        System.out.println("FNASDFNASDLKFNAS");
        String getResult = client.get("/").expect200Ok().body();
        assertThat(getResult).isEqualTo("Server is up and running");
    }
}