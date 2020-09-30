package no.ssb.dapla.blueprintexecution.model;

import io.helidon.common.reactive.Multi;
import io.helidon.common.reactive.Single;

import java.util.*;

public abstract class AbstractJob {

    protected final Set<AbstractJob> previousNodes;
    protected final Set<AbstractJob> nextNodes = new LinkedHashSet<>();

    private final UUID id = UUID.randomUUID();

    private Multi<AbstractJob> previousExecution;
    private Single<AbstractJob> currentExecution;

    protected AbstractJob(AbstractJob... previousNodes) {
        List<AbstractJob> previousNodesList = Arrays.asList(Objects.requireNonNull(previousNodes));
        this.previousNodes = new LinkedHashSet<>(previousNodesList);
        for (AbstractJob previousNode : previousNodesList) {
            previousNode.nextNodes.add(this);
        }
    }

    public UUID getId() {
        return id;
    }

    public synchronized Multi<AbstractJob> getPreviousExecution() {
        if (previousExecution == null) {
            previousExecution = Multi.create(previousNodes).flatMap(AbstractJob::executeJob);
        }
        return previousExecution;
    }

    public synchronized Single<AbstractJob> getCurrentExecution() {
        if (currentExecution == null) {
            currentExecution = startJob();
        }
        return currentExecution;
    }

    /**
     * Execute this job after all the previous jobs are done.
     */
    public final synchronized Single<AbstractJob> executeJob() {
        return getPreviousExecution().collectList().flatMapSingle(jobNodes -> getCurrentExecution());
    }

    protected abstract Single<AbstractJob> startJob();

}
