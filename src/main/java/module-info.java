import no.ssb.dapla.blueprintexecution.BlueprintExecutionApplicationBuilder;

module no.ssb.dapla.blueprintexecution {
    requires io.helidon.webserver;
    requires io.helidon.webserver.accesslog;
    requires io.helidon.health;
    requires java.logging;
    requires io.helidon.health.checks;
    requires io.helidon.metrics;
    requires org.slf4j;
    requires no.ssb.helidon.application;
    requires io.helidon.tracing;
    requires java.net.http;

    requires io.helidon.microprofile.config;
    requires io.helidon.grpc.server; // metrics uses provider org.eclipse.microprofile.config.spi.ConfigProviderResolver

    exports no.ssb.dapla.blueprintexecution;

    provides no.ssb.helidon.application.HelidonApplicationBuilder with BlueprintExecutionApplicationBuilder;
}