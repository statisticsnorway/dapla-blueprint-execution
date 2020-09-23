package no.ssb.dapla.blueprintexecution.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Notebook {

    private final String id;
    private final String path;
    private final String commitId;
    private final String fetchUrl;
    private final List<String> outputs;
    private final List<String> inputs;

    @JsonCreator
    public Notebook(
            @JsonProperty("id") String id,
            @JsonProperty("path") String path,
            @JsonProperty("commitId") String commitId,
            @JsonProperty("fetchUrl") String fetchUrl,
            @JsonProperty("outputs") List<String> outputs,
            @JsonProperty("inputs") List<String> inputs) {
        this.id = id;
        this.path = path;
        this.commitId = commitId;
        this.fetchUrl = fetchUrl;
        this.outputs = outputs;
        this.inputs = inputs;
    }

    public String getId() {
        return id;
    }

    public String getFetchUrl() {
        return fetchUrl;
    }

    public String getCommitId() {
        return commitId;
    }

    public String getPath() {
        return path;
    }
}
