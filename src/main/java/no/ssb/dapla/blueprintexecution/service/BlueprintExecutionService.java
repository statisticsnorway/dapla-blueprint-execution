package no.ssb.dapla.blueprintexecution.service;

import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BlueprintExecutionService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(BlueprintExecutionService.class);

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/", this::doTest);
    }

    private void doTest(ServerRequest req, ServerResponse res) {
        LOG.info("Request received!");
        res.send("Server is up and running");
    }
}
