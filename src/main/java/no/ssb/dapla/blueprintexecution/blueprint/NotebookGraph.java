package no.ssb.dapla.blueprintexecution.blueprint;

import java.util.List;

public class NotebookGraph {

    public List<NotebookDetail> nodes;
    public List<Edge> edges;

    public static class Edge {
        public String from;
        public String to;
    }


}
