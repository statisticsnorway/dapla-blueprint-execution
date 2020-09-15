package no.ssb.dapla.blueprintexecution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.helidon.common.http.Http;
import io.helidon.webclient.WebClientResponse;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.util.diff.DiffUtils;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class WebClientResponseAssert extends AbstractAssert<WebClientResponseAssert, WebClientResponse> {

    private static ObjectMapper MAPPER = new ObjectMapper();
    private byte[] cachedContent;


    public WebClientResponseAssert(WebClientResponse actual) {
        super(actual, WebClientResponseAssert.class);
    }

    public static WebClientResponseAssert assertThat(WebClientResponse actual) {
        return new WebClientResponseAssert(actual);
    }

    public WebClientResponseAssert hasJsonContent(String content) {

        try {
            var expectedJson = MAPPER.readValue(content, JsonNode.class);

            isNotNull();
            try {
                byte[] bytesContent = getContent();
                var actualJson = MAPPER.readValue(bytesContent, JsonNode.class);

                if (!Objects.equals(actualJson, expectedJson)) {
                    var prettyMapper = MAPPER.writerWithDefaultPrettyPrinter();
                    var prettyExpected = prettyMapper.writeValueAsString(expectedJson);
                    var prettyActual = prettyMapper.writeValueAsString(actualJson);
                    var patch = DiffUtils.diff(
                            prettyExpected.lines().collect(Collectors.toList()),
                            prettyActual.lines().collect(Collectors.toList())
                    );
                    var diff = DiffUtils.generateUnifiedDiff(prettyExpected, prettyActual,
                            prettyExpected.lines().collect(Collectors.toList()),
                            patch,
                            5
                    ).stream().skip(2).collect(Collectors.joining("\n"));
                    failWithMessage("response content did not match with json: \n%s", diff);
                }
            } catch (ExecutionException | InterruptedException e) {
                failWithMessage("could not get response content: %s", e.getMessage());
            } catch (IOException e) {
                failWithMessage("response content was not a valid json: %s", e.getMessage());
            }
        } catch (JsonProcessingException e) {
            failWithMessage("provided content was not a valid json: %s", e.getMessage());
        }
        return this;
    }

    private byte[] getContent() throws ExecutionException, InterruptedException {
        if (cachedContent == null) {
            cachedContent = actual.content().as(byte[].class).get();
        }
        return cachedContent;
    }

    public WebClientResponseAssert hasStatus(Http.Status status) {
        isNotNull();
        if (!Objects.equals(actual.status(), status)) {
            failWithMessage("Expected request status be <%s> but was <%s>", status, actual.status());
        }
        return this;
    }
}
