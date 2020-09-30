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
    private final Instant createdAt = Instant.now();
    private Instant startedAt;
    private String startedBy;
    private Status status = Status.Ready;

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

    public void startAll() {
        status = Status.Running;
    }

    public void cancelAll() {
        status = Status.Cancelled;
    }

    public enum Status {
        Ready, Running, Failed, Done, Cancelled
    }

}
