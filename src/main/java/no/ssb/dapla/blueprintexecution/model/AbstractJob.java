package no.ssb.dapla.blueprintexecution.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.helidon.common.reactive.Multi;
import io.helidon.common.reactive.Single;

import java.time.Instant;
import java.util.*;

public abstract class AbstractJob {

    protected final Set<AbstractJob> previousNodes;
    protected final Set<AbstractJob> nextNodes = new LinkedHashSet<>();

    private final UUID id = UUID.randomUUID();

    private Multi<AbstractJob> previousExecution;
    private Single<AbstractJob> currentExecution;
    private Execution.Status status = Execution.Status.Ready;
    private Instant startedAt;
    private Instant endedAt;
    private Throwable exception;

    protected AbstractJob(AbstractJob... previousNodes) {
        List<AbstractJob> previousNodesList = Arrays.asList(Objects.requireNonNull(previousNodes));
        this.previousNodes = new LinkedHashSet<>(previousNodesList);
        for (AbstractJob previousNode : previousNodesList) {
            previousNode.nextNodes.add(this);
        }
    }

    public Execution.Status getStatus() {
        return status;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getEndedAt() {
        return endedAt;
    }

    public Throwable getException() {
        return exception;
    }

    public UUID getId() {
        return id;
    }

    public void setRunning() {
        status = Execution.Status.Running;
        startedAt = Instant.now();
    }

    public void setFailed(Throwable t) {
        status = Execution.Status.Failed;
        endedAt = Instant.now();
        exception = t;
    }

    public void setDone() {
        status = Execution.Status.Done;
        endedAt = Instant.now();
    }

    public void setCancelled() {
        status = Execution.Status.Cancelled;
        endedAt = Instant.now();
    }

    @JsonIgnore
    public synchronized Multi<AbstractJob> getPreviousExecution() {
        if (previousExecution == null) {
            previousExecution = Multi.create(previousNodes).flatMap(AbstractJob::executeJob);
        }
        return previousExecution;
    }

    @JsonIgnore
    public synchronized Single<AbstractJob> getCurrentExecution() {
        if (currentExecution == null) {
            currentExecution = startJob();
        }
        return currentExecution;
    }

    /**
     * Execute this job after all the previous jobs are done.
     */
    @JsonIgnore
    public final synchronized Single<AbstractJob> executeJob() {
        return getPreviousExecution()
                .collectList()
                .onComplete(this::setRunning)
                .flatMapSingle(jobNodes -> getCurrentExecution())
                .onCancel(this::setCancelled)
                .onError(this::setFailed)
                .onComplete(this::setDone);
    }

    protected abstract Single<AbstractJob> startJob();

}
