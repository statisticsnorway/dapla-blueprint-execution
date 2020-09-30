package no.ssb.dapla.blueprintexecution.blueprint;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public class NotebookGraph {

    public Set<NotebookDetail> nodes = new LinkedHashSet<>();
    public Set<Edge> edges = new LinkedHashSet<>();

    public static class Edge {
        public String from;
        public String to;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Edge edge = (Edge) o;
            return from.equals(edge.from) &&
                    to.equals(edge.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, to);
        }
    }


}
