package no.ssb.dapla.blueprintexecution;

import no.ssb.helidon.application.DefaultHelidonApplicationBuilder;
import no.ssb.helidon.application.HelidonApplication;

import static java.util.Optional.ofNullable;

public class BlueprintExecutionApplicationBuilder extends DefaultHelidonApplicationBuilder {
    @Override
    public HelidonApplication build() {
        return new BlueprintExecutionApplication(ofNullable(this.config)
                .orElseGet(DefaultHelidonApplicationBuilder::createDefaultConfig));
    }
}
