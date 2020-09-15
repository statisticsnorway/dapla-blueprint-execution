package no.ssb.dapla.blueprintexecution;

import io.helidon.config.Config;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;

import static io.helidon.config.ConfigSources.classpath;

public class HelidonConfigExtension extends TypeBasedParameterResolver<Config> {
    @Override
    public Config resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        // TODO: Investigate to see if getting the config from another extension is possible.
        return Config.builder(
                classpath("application-dev.yaml"),
                classpath("application.yaml")
        ).metaConfig().build();
    }
}
