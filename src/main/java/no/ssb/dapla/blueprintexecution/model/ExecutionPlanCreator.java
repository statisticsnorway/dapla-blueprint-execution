package no.ssb.dapla.blueprintexecution.model;

import no.ssb.dapla.blueprintexecution.blueprint.NotebookDetail;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public class ExecutionPlanCreator implements Iterable<NotebookDetail> {

    private final DirectedAcyclicGraph<NotebookDetail, DefaultEdge> dag;

    public ExecutionPlanCreator(NotebookGraph graph) {
        Map<String, NotebookDetail> notebookById = graph.nodes.stream()
                .collect(Collectors.toMap(
                        notebook -> notebook.id,
                        notebook -> notebook
                ));
        this.dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
        graph.nodes.forEach(dag::addVertex);
        graph.edges.forEach(edge -> dag.addEdge(notebookById.get(edge.from), notebookById.get(edge.to)));
    }

    @Override
    public Iterator<NotebookDetail> iterator() {
        return new TopologicalOrderIterator<>(dag);
    }

    public Set<NotebookDetail> getAncestors(NotebookDetail notebook) {
        return dag.incomingEdgesOf(notebook).stream()
                .map(dag::getEdgeSource)
                .collect(toSet());
    }

    public Set<NotebookDetail> getDescendants(NotebookDetail notebook) {
        return dag.outgoingEdgesOf(notebook).stream()
                .map(dag::getEdgeTarget)
                .collect(toSet());
    }

    public int getOutDegreeOf(NotebookDetail notebook) {
        return dag.outDegreeOf(notebook);
    }

    public int getInDegreeOf(NotebookDetail notebook) {
        return dag.inDegreeOf(notebook);
    }
}
