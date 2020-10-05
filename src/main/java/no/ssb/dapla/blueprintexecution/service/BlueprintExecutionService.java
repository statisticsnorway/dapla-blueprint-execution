package no.ssb.dapla.blueprintexecution.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.helidon.common.http.Http;
import io.helidon.common.reactive.Multi;
import io.helidon.config.Config;
import io.helidon.media.common.MessageBodyReadableContent;
import io.helidon.webserver.*;
import no.ssb.dapla.blueprintexecution.blueprint.*;
import no.ssb.dapla.blueprintexecution.k8s.K8sExecutionJob;
import no.ssb.dapla.blueprintexecution.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class BlueprintExecutionService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(BlueprintExecutionService.class);


    private final Config config;
    private final ObjectMapper mapper = new ObjectMapper();

    // Keep all the executions in memory for now.
    private final Map<UUID, Execution> executionsMap = new LinkedHashMap<>();

    // Executes the jobs.
    private final ExecutorService jobExecutor = Executors.newCachedThreadPool();

    // Http blueprint client.
    private final BlueprintClient blueprintClient;

    private final KubernetesClient client = new DefaultKubernetesClient();

    public BlueprintExecutionService(Config config) {
        this.config = config;
        this.blueprintClient = new BlueprintClient(config);
    }

    @Override
    public void update(Routing.Rules rules) {
        rules
                .get("/status", this::doTest)
                // Create the execution
                .post("/execute", Handler.create(ExecutionRequest.class, this::doPostExecute))
                .post("/execution", Handler.create(ExecutionRequest.class, this::doPostExecute))
                // Edit the execution (add remove notebooks)
                .put("/execution/{executionId}", Handler.create(ExecutionRequest.class, this::doPutExecution))
                // Get the execution
                .get("/execution/{executionId}", this::doGetExecution)
                // Start the execution
                .post("/execution/{executionId}/start", this::doPostExecutionStart)
                // Cancel the execution (and all jobs)
                .put("/execution/{executionId}/cancel", this::doPutExecutionCancel)
                // List the jobs
                .get("/execution/{executionId}/job/{jobId}", this::doGetExecutionJob)
                // Get log for a job
                .get("/execution/{executionId}/job/{jobId}/log", this::doGetExecutionJobLog)
                // Cancel a job
                .put("/execution/{executionId}/job/{jobId}/cancel", this::doPutExecutionJobCancel)

                .put("/execute", Handler.create(byte[].class, this::doExecute));
    }

    private UUID parseUUIDOrThrow(ServerRequest request, String name) throws BadRequestException {
        var param = request.path().param(name);
        try {
            return UUID.fromString(param);
        } catch (IllegalArgumentException iae) {
            throw new BadRequestException(param + " was not a valid uuid", iae);
        }
    }

    private Execution getExecutionOrThrow(ServerRequest request) throws NotFoundException {
        var executionUUID = parseUUIDOrThrow(request, "executionId");
        if (!executionsMap.containsKey(executionUUID)) {
            throw new NotFoundException("no execution with id " + executionUUID);
        }
        return executionsMap.get(executionUUID);
    }

    private AbstractJob getJobOrThrow(ServerRequest request) throws NotFoundException {
        var execution = getExecutionOrThrow(request);
        var jobUUID = parseUUIDOrThrow(request, "jobId");
        var optionalJob = execution.getJobs().stream().filter(job -> {
            return jobUUID.equals(job.getId());
        }).findFirst();
        return optionalJob.orElseThrow(() -> new NotFoundException("no job with id " + jobUUID));
    }

    private void doPostExecute(ServerRequest request, ServerResponse response, ExecutionRequest executionRequest) {

        try {
            NotebookGraph graph = blueprintClient.getNotebookGraph(executionRequest.repositoryId, executionRequest.commitId,
                    executionRequest.notebookIds);

            ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(graph);

            var execution = new Execution();

            // Convert all the notebooks
            Map<NotebookDetail, KubernetesJob> jobs = new LinkedHashMap<>();
            for (NotebookDetail notebook : executionPlanCreator) {
                jobs.put(notebook, new KubernetesJob(jobExecutor, notebook, config, client));
            }

            // Second pass to setup dependencies.
            for (NotebookDetail notebook : jobs.keySet()) {
                KubernetesJob job = jobs.get(notebook);
                for (NotebookDetail descendant : executionPlanCreator.getAncestors(notebook)) {
                    job.addPrevious(jobs.get(descendant));
                }
            }

            // Find the last jobs
            for (NotebookDetail notebook : jobs.keySet()) {
                if (executionPlanCreator.getOutDegreeOf(notebook) == 0) {
                    execution.addStartingJob(jobs.get(notebook));
                }
            }

            execution.getJobs().addAll(jobs.values());

            executionsMap.put(execution.getId(), execution);

            response.headers().location(URI.create("/api/v1/execution/" + execution.getId()));
            response.status(Http.Status.CREATED_201);
            response.send(execution);
        } catch (ExecutionException | InterruptedException e) {
            response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(e);
        }


    }


    private void doPutExecution(ServerRequest request, ServerResponse response, ExecutionRequest executionRequest) {
        var execution = getExecutionOrThrow(request);
        if (execution.getStatus() != Execution.Status.Ready) {
            response.status(Http.Status.CONFLICT_409).send("Cannot change a started execution");
        } else {
            // Maybe check that repositoryId and commitId are the same?

            // Modify the execution.

        }
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
            // Start the execution in another "control" thread.
            CompletableFuture.runAsync(() -> {
                Multi.create(execution.getStartingJobs())
                        .flatMap(AbstractJob::executeJob)
                        .collectList()
                        .onComplete(execution::setDone)
                        .onError(execution::setFailed)
                        .onCancel(execution::setCancelled)
                        .await();
            }, jobExecutor);
            execution.setRunning();
            response.status(Http.Status.NO_CONTENT_204).send();
        }
    }

    private void doPutExecutionCancel(ServerRequest request, ServerResponse response) {
        var execution = getExecutionOrThrow(request);
        if (execution.getStatus() != Execution.Status.Running) {
            response.status(Http.Status.CONFLICT_409).send("Not running");
        } else {
            execution.setCancelled();
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

            // Get repositories from Blueprint
            var repositories = blueprintClient.getRepositories();
            // Find repoId from payload
            var repository = repositories.stream().findFirst().orElseThrow();

            // Get latest commit from Blueprint
            var commits = blueprintClient.getCommits(repository.id);
            var commit = commits.stream().min(Comparator.comparing(c -> c.createdAt)).orElseThrow();

            // Get all notebooks in given repo from Blueprint
            LOG.debug("Fetching notebooks for repo {} with id {} and commitId {}", repoName, repository.id,
                    commit.id);

            var notebooks = blueprintClient.getNotebooks(repository.id, commit.id);
            var notebook = notebooks.stream().filter(n -> notebookPath.equals(n.path))
                    .findFirst().orElseThrow();

            // Get DAG based on given notebookId
            var notebookGraph = blueprintClient.getNotebookGraph(repository.id, commit.id, notebook.id);

            // get notebookID from request (use HEAD in v1)
            // call blueprint /repository/{repoID}/revisions/{head}/notebooks/{notebookID}

            // Build execution plan
            ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(notebookGraph);
            List<String> executionPlan = StreamSupport.stream(executionPlanCreator.spliterator(), false)
                    .map(n -> n.fetchUrl)
                    .collect(Collectors.toList());

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
