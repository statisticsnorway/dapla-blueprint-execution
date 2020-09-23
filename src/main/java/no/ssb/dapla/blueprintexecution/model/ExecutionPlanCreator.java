package no.ssb.dapla.blueprintexecution.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class ExecutionPlanCreator {

    private List<Notebook> notebooks;
    private List<Edge> edges;
    private final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionPlanCreator.class);

    public ExecutionPlanCreator(JsonNode payload) {
        extractGraph(payload);
    }

    private void extractGraph(JsonNode payload) {
        try {
            notebooks = mapper.readValue(mapper.writeValueAsString(payload.get("nodes")), new TypeReference<>() {});
            edges = mapper.readValue(mapper.writeValueAsString(payload.get("edges")), new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            LOG.error("Error parsing DAG payload", e);
        }
    }

    public List<String> createExecutionPlan() {
        List<String> executionList = new ArrayList<>();
        // TODO create execution plan from edges

        // Create Graph
        Graph<Notebook, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);

        // Add nodes / vertices
        notebooks.forEach(graph::addVertex);
        edges.forEach(edge -> {

            Optional<Notebook> optionalSource = notebooks.stream().filter(
                    nb -> nb.getId().equals(edge.getFrom())).findFirst();
            Notebook source = optionalSource.orElseThrow();
            Optional<Notebook> optionalTarget = notebooks.stream().filter(
                    nb -> nb.getId().equals(edge.getTo())).findFirst();
            Notebook target = optionalTarget.orElseThrow();
            graph.addEdge(source, target);
        });
        Iterator<Notebook> iterator = new TopologicalOrderIterator<>(graph);
        while (iterator.hasNext()) {
            executionList.add(iterator.next().getFetchUrl());
        }
        return executionList;
    }

    public List<Notebook> getNotebooks() {
        return notebooks;
    }

    public List<Edge> getEdges() {
        return edges;
    }
}
