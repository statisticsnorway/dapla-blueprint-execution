package no.ssb.dapla.blueprintexecution.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecutionPlanCreatorTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private static final ClassLoader classloader = Thread.currentThread().getContextClassLoader();

    @Test
    void testPayloadParsing() throws Exception{

        JsonNode jsonNode = mapper.readTree(classloader.getResourceAsStream("dag.json"));
        ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(jsonNode);
        assertThat(executionPlanCreator.getNotebooks().size()).isEqualTo(4);
        assertThat(executionPlanCreator.getEdges().size()).isEqualTo(5);
    }

    @Test
    void thatCyclicGraphFails() throws Exception{
        JsonNode dag = mapper.readTree(classloader.getResourceAsStream("dag_invalid.json"));
        ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(dag);
        assertThatThrownBy(executionPlanCreator::createExecutionPlan).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCreateExecutionPlan() throws Exception {
        JsonNode dag = mapper.readTree(classloader.getResourceAsStream("dag.json"));
        ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(dag);
        List<String> executionPlan = executionPlanCreator.createExecutionPlan();

        // Get correct plan from resources/executionPlan.json
        JsonNode correctPlanJson = mapper.readTree(classloader.getResourceAsStream("executionPlan.json"));
        List<Notebook> correctPlan = mapper.readValue(mapper.writeValueAsString(correctPlanJson.get("nodes")), new TypeReference<>() {});
        assertThat(executionPlan).isEqualTo(correctPlan.stream().map(Notebook::getFetchUrl).collect(Collectors.toList()));
    }
}