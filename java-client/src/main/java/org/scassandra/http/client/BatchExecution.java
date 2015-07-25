package org.scassandra.http.client;

import java.util.List;

public final class BatchExecution {

    private final List<BatchStatement> statements;
    private final String consistency;

    public BatchExecution(List<BatchStatement> statements, String consistency) {
        this.statements = statements;
        this.consistency = consistency;
    }

    private List<BatchStatement> getStatements() {
        return statements;
    }

    private String getConsistency() {
        return consistency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BatchExecution that = (BatchExecution) o;

        if (statements != null ? !statements.equals(that.statements) : that.statements != null) return false;
        return !(consistency != null ? !consistency.equals(that.consistency) : that.consistency != null);

    }

    @Override
    public int hashCode() {
        int result = statements != null ? statements.hashCode() : 0;
        result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BatchExecution{" +
                "statements=" + statements +
                ", consistency='" + consistency + '\'' +
                '}';
    }
}
