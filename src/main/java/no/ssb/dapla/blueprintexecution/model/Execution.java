package no.ssb.dapla.blueprintexecution.model;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * Represent a execution.
 */
public class Execution {

    private final UUID id = UUID.randomUUID();
    private final List<AbstractJob> jobs = new LinkedList<>();
    private final List<AbstractJob> endJobs = new LinkedList<>();
    private final List<AbstractJob> startJobs = new LinkedList<>();
    private final Instant createdAt = Instant.now();
    private String commitId;
    private String repositoryId;
    private Instant startedAt;
    private Instant endedAt;
    private String startedBy;
    private Status status = Status.Ready;
    private Throwable exception;

    public List<AbstractJob> getStartJobs() {
        return startJobs;
    }

    public List<AbstractJob> getEndJobs() {
        return endJobs;
    }

    public String getRepositoryId() {
        return repositoryId;
    }

    public void setRepositoryId(String repositoryId) {
        this.repositoryId = repositoryId;
    }

    public String getCommitId() {
        return commitId;
    }

    public void setCommitId(String commitId) {
        this.commitId = commitId;
    }


    public Instant getCreatedAt() {
        return createdAt;
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

    public String getStartedBy() {
        return startedBy;
    }

    public void setStartedBy(String startedBy) {
        this.startedBy = startedBy;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public List<AbstractJob> getJobs() {
        return jobs;
    }

    public Status getStatus() {
        return status;
    }

    public void addEndJob(AbstractJob job) {
        this.endJobs.add(job);
    }

    public void addStartJob(AbstractJob job) {
        this.startJobs.add(job);
    }

    public void setRunning() {
        startedAt = Instant.now();
        status = Status.Running;
    }

    public void setCancelled() {
        endedAt = Instant.now();
        status = Status.Cancelled;
    }

    public void setFailed(Throwable t) {
        endedAt = Instant.now();
        status = Status.Failed;
        exception = t;
    }

    public void setDone() {
        endedAt = Instant.now();
        status = Status.Done;
    }

    public enum Status {
        Ready, Running, Failed, Done, Cancelled
    }

}
