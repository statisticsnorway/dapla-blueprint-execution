package no.ssb.dapla.blueprintexecution.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Represent a execution.
 */
public class Execution {

    private final UUID id = UUID.randomUUID();

    private String startedBy;
    private Instant startedAt;
    private List<AbstractJob> jobs;
    private Status status;

    enum Status {
        Running, Failed, Done
    }

}
