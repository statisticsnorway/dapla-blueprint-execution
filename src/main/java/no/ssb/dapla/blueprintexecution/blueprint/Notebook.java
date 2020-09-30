package no.ssb.dapla.blueprintexecution.blueprint;

import java.util.Objects;

public class Notebook {
    public String id;
    public String path;
    public String commitId;
    public String fetchUrl;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Notebook notebook = (Notebook) o;
        return id.equals(notebook.id) &&
                commitId.equals(notebook.commitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, commitId);
    }
}
