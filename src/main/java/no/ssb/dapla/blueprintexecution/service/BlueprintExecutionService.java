package no.ssb.dapla.blueprintexecution.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.helidon.common.http.Http;
import io.helidon.common.http.MediaType;
import io.helidon.config.Config;
import io.helidon.media.common.MessageBodyReadableContent;
import io.helidon.media.jackson.JacksonSupport;
import io.helidon.webclient.WebClient;
import io.helidon.webclient.WebClientResponse;
import io.helidon.webserver.*;
import no.ssb.dapla.blueprintexecution.k8s.K8sExecutionJob;
import no.ssb.dapla.blueprintexecution.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.StreamSupport;


public class BlueprintExecutionService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(BlueprintExecutionService.class);

    private final Config config;
    private final ObjectMapper mapper = new ObjectMapper();

    // Keep all the executions in memory for now.
    private final Map<UUID, Execution> executionsMap = new LinkedHashMap<>();

    // Executes the jobs.
    private final Executor jobExecutor = Executors.newCachedThreadPool();

    public BlueprintExecutionService(Config config) {
        this.config = config;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules
                .get("/status", this::doTest)
                .post("/execute", Handler.create(ExecutionRequest.class, this::doPostExecute))
                .post("/execution/{executionId}/start", this::doPostExecutionStart)
                .put("/execution/{executionId}/cancel", this::doPutExecutionCancel)
                .get("/execution/{executionId}", this::doGetExecution)
                .get("/execution/{executionId}/job/{jobId}", this::doGetExecutionJob)
                .get("/execution/{executionId}/job/{jobId}/log", this::doGetExecutionJobLog)
                .put("/execution/{executionId}/job/{jobId}/cancel", this::doPutExecutionJobCancel)

                .put("/execute", Handler.create(byte[].class, this::doExecute));
    }

    private Execution getExecutionOrThrow(ServerRequest request) throws NotFoundException {
        var executionId = request.path().param("executionId");
        var executionUUID = UUID.fromString(executionId);
        if (!executionsMap.containsKey(executionUUID)) {
            throw new NotFoundException("no execution with id " + executionId);
        }
        return executionsMap.get(executionUUID);
    }

    private AbstractJob getJobOrThrow(ServerRequest request) throws NotFoundException {
        var jobId = request.path().param("jobId");
        try {
            var execution = getExecutionOrThrow(request);
            var jobUUID = UUID.fromString(jobId);
            var optionalJob = execution.getJobs().stream().filter(job -> {
                return jobUUID.equals(job.getId());
            }).findFirst();
            return optionalJob.orElseThrow(() -> new NotFoundException("no job with id " + jobId));
        } catch (IllegalArgumentException iae) {
            throw new BadRequestException("could not parse uuid " + jobId, iae);
        }
    }

    private void doPostExecute(ServerRequest request, ServerResponse response, ExecutionRequest executionRequest) {
        var execution = new Execution();
        executionsMap.put(execution.getId(), execution);
        response.headers().location(URI.create("/api/v1/execution/" + execution.getId()));
        response.status(Http.Status.CREATED_201);
        response.send();
    }

    private void doGetExecution(ServerRequest request, ServerResponse response) {
        var execution = getExecutionOrThrow(request);
        response.status(Http.Status.OK_200).send(execution);
    }

    private void doPostExecutionStart(ServerRequest request, ServerResponse response) {
        var execution = getExecutionOrThrow(request);
        if (execution.getStatus() != Execution.Status.Ready) {
            response.status(Http.Status.CONFLICT_409).send("Not ready");
        } else {
            execution.startAll();
            response.status(Http.Status.NO_CONTENT_204).send();
        }
    }

    private void doPutExecutionCancel(ServerRequest request, ServerResponse response) {
        var execution = getExecutionOrThrow(request);
        if (execution.getStatus() != Execution.Status.Running) {
            response.status(Http.Status.CONFLICT_409).send("Not running");
        } else {
            execution.cancelAll();
            response.status(Http.Status.NO_CONTENT_204).send();
        }
    }

    private void doGetExecutionJob(ServerRequest request, ServerResponse response) {
        var job = getJobOrThrow(request);
        response.status(Http.Status.OK_200).send(job);
    }

    private void doGetExecutionJobLog(ServerRequest request, ServerResponse response) {
        var job = getJobOrThrow(request);
        response.status(Http.Status.OK_200).send(); //job.getLog());
    }

    private void doPutExecutionJobCancel(ServerRequest request, ServerResponse response) {
        var job = getJobOrThrow(request);
        response.status(Http.Status.NO_CONTENT_204).send(); // job.cancel());
    }

    private void doTest(ServerRequest req, ServerResponse res) {
        LOG.info("Request received!");
        res.send("Server is up and running");
    }

    @Deprecated
    private void doExecute(ServerRequest req, ServerResponse res, byte[] body) {
        // Workflow:
        // Choose repo (provide name in json to this endpoint, get ID from blueprint service):
        // GET blueprint/repositories

        // Get the latest HEAD commitID:
        // GET blueprint/repositories/{repoId}/commits/HEAD

        // Choose the notebook to start from (provide name/path in json to this endpoint, get ID from blueprint service):
        // GET blueprint/repositories/{repoId}/commits/{commitId}/notebooks

        // Get DAG
        // GET blueprint/repositories/{repoId}/commits/{commitId}/notebooks/{notebookId}

        try {
            LOG.debug("Start building and executing Notebook pipeline");

            // Get and validate payload params
            JsonNode node = mapper.readTree(body);
            String repoName = Objects.requireNonNull(node.get("repo").asText(), "Repository URL is required");
            repoName += ".git"; // URI from Blueprint service contains git extension
            String notebookPath = Objects.requireNonNull(node.get("notebook").asText(), "Notebook path is required");


            String blueprintBaseUrl = config.get("blueprint.url").asString().get();
            WebClient client = WebClient.builder()
                    .baseUri(blueprintBaseUrl)
                    .addMediaSupport(JacksonSupport.create(mapper))
                    .build();

            // Get repositories from Blueprint
            WebClientResponse repsRes = client.get()
                    .path("repositories")
                    .accept(MediaType.APPLICATION_JSON)
                    .submit().get();
            if (repsRes.status().equals(Http.Status.INTERNAL_SERVER_ERROR_500)) {
                throw new HttpException(String.format("Error calling %s", repsRes.lastEndpointURI().toString()), repsRes.status());
            }

            // Find repoId from payload
            String repoId = extractIdFromJsonArray(repoName, repsRes.content(), "uri");

            // Get latest commit from Blueprint
            WebClientResponse commitsRes = client.get()
                    .path(String.format("repositories/%s/commits", repoId))
                    .accept(MediaType.APPLICATION_JSON)
                    .submit().get();
            if (commitsRes.status().equals(Http.Status.INTERNAL_SERVER_ERROR_500)) {
                throw new HttpException(String.format("Error calling %s", commitsRes.lastEndpointURI().toString()), commitsRes.status());
            }
            // TODO pick first commit in list for now. We need a way to find latest commit, createdAt is for now null
            JsonNode commits = commitsRes.content().as(JsonNode.class).toCompletableFuture().join();
            String commitID = StreamSupport.stream(commits.spliterator(), false)
                    .sorted(Comparator.comparing(jsonNode -> jsonNode.get("createdAt").asInt()))
                    .map(jsonNode -> jsonNode.get("id").asText())
                    .findFirst().orElseThrow();


            // Get all notebooks in given repo from Blueprint
            LOG.debug("Fetching notebooks for repo {} with id {} and commitId {}", repoName, repoId, commitID);
            WebClientResponse notebookRes = client.get()
                    .path(String.format("repositories/%s/commits/%s/notebooks", repoId, commitID))
                    .accept(MediaType.APPLICATION_JSON)
                    .submit().get();
            if (notebookRes.status().equals(Http.Status.INTERNAL_SERVER_ERROR_500)) {
                throw new HttpException(String.format("Error calling %s", notebookRes.lastEndpointURI().toString()), notebookRes.status());
            }
            String notebookId = extractIdFromJsonArray(notebookPath, notebookRes.content(), "path");

            // Get DAG based on given notebookId
            WebClientResponse blueprintRes = client.get()
                    .path(String.format("/repositories/%s/commits/%s/notebooks/%s", repoId, commitID, notebookId))
                    .accept(MediaType.create(
                            "application", "vnd.ssb.blueprint.dag+json"))
                    .submit().get();
            if (blueprintRes.status().equals(Http.Status.INTERNAL_SERVER_ERROR_500)) {
                throw new HttpException(String.format("Error calling %s", blueprintRes.lastEndpointURI().toString()), blueprintRes.status());
            }
            JsonNode responseBody = blueprintRes.content().as(JsonNode.class).toCompletableFuture().join();

            // get notebookID from request (use HEAD in v1)
            // call blueprint /repository/{repoID}/revisions/{head}/notebooks/{notebookID}

            // Build execution plan
            ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(responseBody);
            List<String> executionPlan = executionPlanCreator.createExecutionPlan();

            // Build k8s job list
            List<K8sExecutionJob> k8sExecutionJobs = new ArrayList<>();
            executionPlan.forEach(notebookUri -> k8sExecutionJobs.add(new K8sExecutionJob(notebookUri)));

            // TODO execute each job and wait for it to finish before starting next
            k8sExecutionJobs.forEach(K8sExecutionJob::createAndRunJOb); // Which means, not like this

            //        // Create and run job async
            //        CompletableFuture.runAsync(() -> k8sExecutionJob.createAndRunJOb())
            //                .orTimeout(20, TimeUnit.SECONDS);
            res.status(Http.Status.CREATED_201).send();
        } catch (HttpException e) {
            LOG.error("Error calling Blueprint service", e);
            res.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(e.toString());
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error getting notebook from Blueprint", e);
            res.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(e.toString());
        } catch (IOException e) {
            LOG.error("Error", e);
            res.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(e.toString());
        }
    }

    private String extractIdFromJsonArray(String compareValue, MessageBodyReadableContent content, String nodeName) {
        JsonNode repsResBody = content.as(JsonNode.class).toCompletableFuture().join();
        String result = null;
        for (JsonNode repoNode : repsResBody) {
            if (repoNode.get(nodeName).asText().equals(compareValue)) {
                result = repoNode.get("id").asText();
                break;
            }
        }
        return result;
    }

}
