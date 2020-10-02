package no.ssb.dapla.blueprintexecution.blueprint;

import io.helidon.common.GenericType;
import io.helidon.common.http.MediaType;
import io.helidon.config.Config;
import io.helidon.media.jackson.JacksonSupport;
import io.helidon.webclient.WebClient;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class BlueprintClient {

    private static final String BLUEPRINT_URL = "blueprint.url";

    private static final String REPOSITORIES_PATH = "/api/v1/repositories";
    private static final String COMMITS_PATH = "/api/v1/repositories/%s/commits";
    private static final String NOTEBOOKS_PATH = "/api/v1/repositories/%s/commits/%s/notebooks";
    private static final String NOTEBOOK_PATH = "/api/v1/repositories/%s/commits/%s/notebooks/%s";

    private static final GenericType<List<Repository>> REPOSITORY_LIST = new GenericType<>() {
    };
    private static final GenericType<List<Commit>> COMMIT_LIST = new GenericType<>() {
    };
    private static final GenericType<List<Notebook>> NOTEBOOK_LIST = new GenericType<>() {
    };
    private static final Class<NotebookGraph> NOTEBOOK_GRAPH = NotebookGraph.class;

    private static final MediaType APPLICATION_BLUEPRINT_DAG = MediaType.create("application",
            "vnd.ssb.blueprint.dag+json");


    private final WebClient client;

    public BlueprintClient(Config config) {
        Objects.requireNonNull(config);
        client = WebClient.builder()
                .baseUri(config.get(BLUEPRINT_URL).asString().get())
                .addMediaSupport(JacksonSupport.create())
                .build();
    }

    public List<Repository> getRepositories() throws ExecutionException, InterruptedException {
        return client.get()
                .accept(MediaType.APPLICATION_JSON)
                .path(REPOSITORIES_PATH)
                .request(REPOSITORY_LIST).get();
    }

    public Repository getRepository(String repositoryId) throws ExecutionException, InterruptedException {
        return getRepositories().stream().filter(repository -> repositoryId.equals(repository.id))
                .findFirst().orElseThrow();
    }

    public List<Commit> getCommits(String repositoryId) throws ExecutionException, InterruptedException {
        return client.get()
                .accept(MediaType.APPLICATION_JSON)
                .path(String.format(COMMITS_PATH, repositoryId))
                .request(COMMIT_LIST).get();
    }

    public List<Notebook> getNotebooks(String repositoryId, String commitId) throws ExecutionException, InterruptedException {
        return client.get()
                .accept(MediaType.APPLICATION_JSON)
                .path(String.format(NOTEBOOKS_PATH, repositoryId, commitId))
                .request(NOTEBOOK_LIST).get();
    }

    public NotebookGraph getNotebookGraph(String repositoryId, String commitId, String notebookId) throws ExecutionException, InterruptedException {
        return client.get()
                .accept(APPLICATION_BLUEPRINT_DAG)
                .path(String.format(NOTEBOOK_PATH, repositoryId, commitId, notebookId))
                .request(NOTEBOOK_GRAPH).get();
    }

    public NotebookGraph getNotebookGraph(String repositoryId, String commitId, Set<String> notebookIds) throws ExecutionException, InterruptedException {
        // The notebook service does not support asking for many notebook ids. We emulate this for now by making
        // many calls on notebook that are not yet seen in the previous graphs.
        Set<String> seenNotebooks = new HashSet<>();
        NotebookGraph aggregatedGraph = new NotebookGraph();
        for (String notebookId : notebookIds) {
            if (!seenNotebooks.contains(notebookId)) {
                NotebookGraph notebookGraph = getNotebookGraph(repositoryId, commitId, notebookId);
                notebookGraph.nodes.stream().map(n -> n.id).forEach(seenNotebooks::add);
                aggregatedGraph.nodes.addAll(notebookGraph.nodes);
                aggregatedGraph.edges.addAll(notebookGraph.edges);
            }
        }
        return aggregatedGraph;
    }
}
