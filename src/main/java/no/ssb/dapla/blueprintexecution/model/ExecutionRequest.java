package no.ssb.dapla.blueprintexecution.model;

import java.util.Set;

public class ExecutionRequest {
    public String repositoryId;
    public String commitId;
    public Set<String> notebookIds;
}
