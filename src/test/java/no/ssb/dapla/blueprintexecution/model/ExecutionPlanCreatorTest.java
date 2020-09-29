package no.ssb.dapla.blueprintexecution.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookDetail;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookGraph;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecutionPlanCreatorTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private static final ClassLoader classloader = Thread.currentThread().getContextClassLoader();

    @Test
    void testPayloadParsing() throws Exception{
        NotebookGraph graph = mapper.readValue(classloader.getResourceAsStream("dag.json"), NotebookGraph.class);
        assertThat(graph.nodes.size()).isEqualTo(4);
        assertThat(graph.edges.size()).isEqualTo(5);
    }

    @Test
    void thatCyclicGraphFails() throws Exception{
        NotebookGraph graph = mapper.readValue(classloader.getResourceAsStream("dag_invalid.json"), NotebookGraph.class);
        assertThatThrownBy(() -> {
            ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(graph);
            executionPlanCreator.iterator();
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCreateExecutionPlan() throws Exception {
        NotebookGraph graph = mapper.readValue(classloader.getResourceAsStream("dag.json"), NotebookGraph.class);
        Iterable<NotebookDetail> executionPlan = new ExecutionPlanCreator(graph);

        // Get correct plan from resources/executionPlan.json
        Iterable<NotebookDetail> expectedPlan = mapper.readValue(
                classloader.getResourceAsStream("executionPlan.json"), new TypeReference<>() {});

        assertThat(executionPlan).usingElementComparatorOnFields("id")
                .containsAll(expectedPlan);
    }
}