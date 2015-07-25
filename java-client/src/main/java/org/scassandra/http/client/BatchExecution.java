package org.scassandra.http.client;

import java.util.List;

public final class BatchExecution {

    private final List<BatchStatement> batchStatements;
    private final String consistency;

    public BatchExecution(List<BatchStatement> batchStatements, String consistency) {
        this.batchStatements = batchStatements;
        this.consistency = consistency;
    }

    public List<BatchStatement> getBatchStatements() {
        return batchStatements;
    }

    public String getConsistency() {
        return consistency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BatchExecution that = (BatchExecution) o;

        if (batchStatements != null ? !batchStatements.equals(that.batchStatements) : that.batchStatements != null) return false;
        return !(consistency != null ? !consistency.equals(that.consistency) : that.consistency != null);

    }

    @Override
    public int hashCode() {
        int result = batchStatements != null ? batchStatements.hashCode() : 0;
        result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BatchExecution{" +
                "batchStatements=" + batchStatements +
                ", consistency='" + consistency + '\'' +
                '}';
    }
}
