package no.ssb.dapla.blueprintexecution.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Edge {

    private final String from;
    private final String to;

    @JsonCreator
    public Edge(
            @JsonProperty("from") String from,
            @JsonProperty("to") String to
    ) {
        this.from = from;
        this.to = to;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }
}
