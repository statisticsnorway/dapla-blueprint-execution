package no.ssb.dapla.blueprintexecution.model;

import java.util.LinkedHashSet;
import java.util.Set;

public class ExecutionRequest {
    public String repositoryId;
    public String commitId;
    public Set<String> startNotebookIds = new LinkedHashSet<>();
    public Set<String> endNotebookIds = new LinkedHashSet<>();
}
