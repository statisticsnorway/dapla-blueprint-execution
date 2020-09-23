package no.ssb.dapla.blueprintexecution;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.dapla.blueprintexecution.model.Edge;
import no.ssb.dapla.blueprintexecution.model.Notebook;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

import java.util.List;

public class JsonTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private static final ClassLoader classloader = Thread.currentThread().getContextClassLoader();

    @Test
    @Ignore
    void testJsonArray() throws Exception {
        JsonNode jsonNode = mapper.readTree(classloader.getResourceAsStream("repositories_response.json"));
        String repoUri = "https://github.com/statisticsnorway/dapla-notebooks.git";
        String id = null;
        for (JsonNode node : jsonNode) {
            String uri = node.get("uri").asText();
            if (uri.equals(repoUri)) {
                id = node.get("id").asText();
                break;
            }
            System.out.println(uri);
        }
        System.out.println("ID: " + id);
//        jsonNode.forEach(System.out::println);
    }

    @Test
    @Ignore
    void testSortJsonArray() throws Exception{
        JsonNode jsonNode = mapper.readTree(classloader.getResourceAsStream("commits.json"));
//        ObjectReader reader = mapper.readerFor(new TypeReference<List<String>>() {});
//        List<String> o = reader.readValue(jsonNode);
//        System.out.println(o);
        System.out.println(jsonNode.get(0).get("id"));

    }

    @Test
    @Ignore
    public void testJson() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode topNode = mapper.createObjectNode();

        ObjectNode node1 = mapper.createObjectNode();
        node1.put("id", "nbId1");
        node1.put("fetchUrl", "/api/v1/repositories/repoId/commits/commitId/notebook/foo");
        node1.put("path", "skatt.ipynb");
        ObjectNode node2 = mapper.createObjectNode();
        node2.put("id", "nbId2");
        node2.put("fetchUrl", "/api/v1/repositories/repoId/commits/commitId/notebook/bar");
        node2.put("path", "skatt2.ipynb");

        ObjectNode edge1 = mapper.createObjectNode();
        edge1.put("from", "nbId1");
        edge1.put("to", "nbId2");
        topNode.putArray("node").add(node1).add(node2);
        topNode.putArray("edge").add(edge1);
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(topNode);
        System.out.println(json);

        List<Notebook> notebooks = mapper.readValue(mapper.writeValueAsString(topNode.get("node")), new TypeReference<>() {});
        notebooks.forEach(System.out::println);
        List<Edge> edges = mapper.readValue(mapper.writeValueAsString(topNode.get("edge")), new TypeReference<>() {});
        edges.forEach(System.out::println);
    }
}
