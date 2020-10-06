package no.ssb.dapla.blueprintexecution.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.helidon.common.http.DataChunk;
import io.helidon.common.http.Http;
import io.helidon.common.reactive.Multi;
import io.helidon.config.Config;
import io.helidon.webserver.*;
import no.ssb.dapla.blueprintexecution.blueprint.BlueprintClient;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookDetail;
import no.ssb.dapla.blueprintexecution.blueprint.NotebookGraph;
import no.ssb.dapla.blueprintexecution.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;


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
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

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
                // Get the executions
                .get("/execution", this::doGetExecutions)
                // Get the execution
                .get("/execution/{executionId}", this::doGetExecution)
                // Start the execution
                .post("/execution/{executionId}/start", this::doPostExecutionStart)
                // Cancel the execution (and all jobs)
                .put("/execution/{executionId}/cancel", this::doPutExecutionCancel)
                // List the jobs
                .get("/execution/{executionId}/job", this::doGetExecutionJobs)
                // List the jobs
                .get("/execution/{executionId}/job/{jobId}", this::doGetExecutionJob)
                // Get log for a job
                .get("/execution/{executionId}/job/{jobId}/log", this::doGetExecutionJobLog)
                // Cancel a job
                .put("/execution/{executionId}/job/{jobId}/cancel", this::doPutExecutionJobCancel);
    }

    private void doGetExecutions(ServerRequest request, ServerResponse response) {
        response.status(Http.Status.OK_200).send(executionsMap.values());
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

    private void setupExecution(Execution execution, ExecutionRequest executionRequest) throws ExecutionException, InterruptedException {

        execution.setCommitId(executionRequest.commitId);
        execution.setRepositoryId(executionRequest.repositoryId);

        execution.getJobs().clear();
        execution.getStartingJobs().clear();

        NotebookGraph graph = blueprintClient.getNotebookGraph(executionRequest.repositoryId, executionRequest.commitId,
                executionRequest.notebookIds);

        ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(graph);

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
    }

    private void doPostExecute(ServerRequest request, ServerResponse response, ExecutionRequest executionRequest) {

        try {
            var execution = new Execution();
            setupExecution(execution, executionRequest);
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
            // TODO: Maybe check that repositoryId and commitId are the same?
            try {
                // Modify the execution.
                setupExecution(execution, executionRequest);
                response.status(Http.Status.OK_200).send(execution);

            } catch (ExecutionException | InterruptedException e) {
                response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(e);
            }
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

    private void doGetExecutionJobs(ServerRequest request, ServerResponse response) {
        var execution = getExecutionOrThrow(request);
        response.status(Http.Status.OK_200).send(execution.getJobs());
    }

    private void doGetExecutionJob(ServerRequest request, ServerResponse response) {
        var job = getJobOrThrow(request);
        response.status(Http.Status.OK_200).send(job);
    }

    private void doGetExecutionJobLog(ServerRequest request, ServerResponse response) {
        var job = getJobOrThrow(request);

        Multi<DataChunk> data = ((KubernetesJob) job).getLog()
                .map(line -> DataChunk.create(true, ByteBuffer.wrap(line.getBytes())));

        response.status(Http.Status.OK_200).send(data);
    }

    private void doPutExecutionJobCancel(ServerRequest request, ServerResponse response) {
        var job = getJobOrThrow(request);
        response.status(Http.Status.NO_CONTENT_204).send(); // job.cancel());
    }

    private void doTest(ServerRequest req, ServerResponse res) {
        LOG.info("Request received!");
        res.send("Server is up and running");
    }

}
