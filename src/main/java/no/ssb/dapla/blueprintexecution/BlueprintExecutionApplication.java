package no.ssb.dapla.blueprintexecution;

import io.helidon.config.Config;
import io.helidon.health.HealthSupport;
import io.helidon.health.checks.HealthChecks;
import io.helidon.metrics.MetricsSupport;
import io.helidon.tracing.TracerBuilder;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebTracingConfig;
import io.helidon.webserver.accesslog.AccessLogSupport;
import io.opentracing.Tracer;
import no.ssb.dapla.blueprintexecution.service.BlueprintExecutionService;
import no.ssb.helidon.application.DefaultHelidonApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class BlueprintExecutionApplication extends DefaultHelidonApplication {

    private static final Logger LOG;

    static {
        installSlf4jJulBridge();
        LOG = LoggerFactory.getLogger(BlueprintExecutionApplication.class);
    }

    public BlueprintExecutionApplication(Config config) {
        put(Config.class, config);

        TracerBuilder<?> tracerBuilder = TracerBuilder.create(config.get("tracing")).registerGlobal(false);
        Tracer tracer = tracerBuilder.build();

        HealthSupport health = HealthSupport.builder()
                .addLiveness(HealthChecks.healthChecks())
                .build();
        MetricsSupport metrics = MetricsSupport.create();

        // routing
        Routing routing = Routing.builder()
                .register(AccessLogSupport.create(config.get("webserver.access-log")))
                .register(WebTracingConfig.create(config.get("tracing")))
                .register(health)
                .register(metrics)
                .register("/", new BlueprintExecutionService())
                .build();

        put(Routing.class, routing);

        // web-server
        WebServer webServer = WebServer.create(
                ServerConfiguration.builder(config.get("webserver"))
                        .tracer(tracer)
                        .build(),
                routing);
        put(WebServer.class, webServer);
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        new BlueprintExecutionApplicationBuilder().build().start()
                .toCompletableFuture()
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(app -> LOG.info("Webserver running at port: {}, started in {} ms",
                        app.get(WebServer.class).port(), System.currentTimeMillis() - startTime))
                .exceptionally(throwable -> {
                    LOG.error("While starting the application", throwable);
                    System.exit(1);
                    return null;
                });
    }
}
