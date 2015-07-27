package org.scassandra.http.client;

import java.util.Arrays;
import java.util.List;

public final class BatchExecution {

    private final List<BatchStatement> batchStatements;
    private final String consistency;
    private final BatchType batchType;

    private BatchExecution(List<BatchStatement> batchStatements, String consistency, BatchType batchType) {
        this.batchStatements = batchStatements;
        this.consistency = consistency;
        this.batchType = batchType;
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

        if (batchStatements != null ? !batchStatements.equals(that.batchStatements) : that.batchStatements != null)
            return false;
        if (consistency != null ? !consistency.equals(that.consistency) : that.consistency != null) return false;
        return batchType == that.batchType;

    }

    @Override
    public int hashCode() {
        int result = batchStatements != null ? batchStatements.hashCode() : 0;
        result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
        result = 31 * result + (batchType != null ? batchType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BatchExecution{" +
                "batchStatements=" + batchStatements +
                ", consistency='" + consistency + '\'' +
                ", type=" + batchType +
                '}';
    }

    public static BatchExecutionBuilder builder() {
        return new BatchExecutionBuilder();
    }


    public static class BatchExecutionBuilder {
        private List<BatchStatement> batchStatements;
        private String consistency;
        private BatchType batchType;

        private BatchExecutionBuilder() {
        }

        public BatchExecutionBuilder withBatchStatements(List<BatchStatement> batchStatements) {
            this.batchStatements = batchStatements;
            return this;
        }

        public BatchExecutionBuilder withBatchStatements(BatchStatement... batchStatements) {
            this.batchStatements = Arrays.asList(batchStatements);
            return this;
        }

        public BatchExecutionBuilder withConsistency(String consistency) {
            this.consistency = consistency;
            return this;
        }

        public BatchExecutionBuilder withBatchType(BatchType batchType) {
            this.batchType = batchType;
            return this;
        }

        public BatchExecution build() {
            return new BatchExecution(batchStatements, consistency, batchType);
        }
    }
}
