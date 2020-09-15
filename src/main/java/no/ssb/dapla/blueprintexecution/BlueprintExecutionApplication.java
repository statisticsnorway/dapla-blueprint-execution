package no.ssb.dapla.blueprintexecution;

import ch.qos.logback.classic.util.ContextInitializer;
import io.helidon.config.Config;
import io.helidon.health.HealthSupport;
import io.helidon.health.checks.HealthChecks;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.WebTracingConfig;
import io.helidon.webserver.accesslog.AccessLogSupport;
import no.ssb.dapla.blueprintexecution.service.BlueprintExecutionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

public class BlueprintExecutionApplication {

    private static final Logger LOG;

    static {
        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        LOG = LoggerFactory.getLogger(BlueprintExecutionApplication.class);
    }

    private final Map<Class<?>, Object> instanceByType = new ConcurrentHashMap<>();

    public BlueprintExecutionApplication(Config config) {
        put(Config.class, config);

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
        var webServer = WebServer.builder();
        webServer.routing(routing)
                .config(config.get("webserver"));

        put(WebServer.class, webServer.build());
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        BlueprintExecutionApplication app = new BlueprintExecutionApplication(Config.create());

        app.get(WebServer.class).start()
                .toCompletableFuture()
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(ws -> {
                    LOG.info("Server up and running: http://{}:{}/api/v1, started in {} ms",
                            ws.configuration().bindAddress(), ws.port(), System.currentTimeMillis() - startTime);
                    ws.whenShutdown().thenRun(()
                            -> System.out.println("WEB server is DOWN. Good bye!"));
                })
                .exceptionally(t -> {
                    LOG.error("Startup failed: " + t.getMessage());
                    t.printStackTrace(System.err);
                    System.exit(1);
                    return null;
                });
    }

    public <T> T put(Class<T> clazz, T instance) {
        return (T) instanceByType.put(clazz, instance);
    }

    public <T> T get(Class<T> clazz) {
        return (T) instanceByType.get(clazz);
    }
}
