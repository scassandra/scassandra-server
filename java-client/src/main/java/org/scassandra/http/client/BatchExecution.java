package org.scassandra.http.client;

import java.util.Arrays;
import java.util.List;

public final class BatchExecution {

    private final List<BatchQuery> batchQueries;
    private final String consistency;
    private final BatchType batchType;

    private BatchExecution(List<BatchQuery> batchQueries, String consistency, BatchType batchType) {
        this.batchQueries = batchQueries;
        this.consistency = consistency;
        this.batchType = batchType;
    }

    public List<BatchQuery> getBatchQueries() {
        return batchQueries;
    }

    public String getConsistency() {
        return consistency;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BatchExecution that = (BatchExecution) o;

        if (batchQueries != null ? !batchQueries.equals(that.batchQueries) : that.batchQueries != null)
            return false;
        if (consistency != null ? !consistency.equals(that.consistency) : that.consistency != null) return false;
        return batchType == that.batchType;

    }

    @Override
    public int hashCode() {
        int result = batchQueries != null ? batchQueries.hashCode() : 0;
        result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
        result = 31 * result + (batchType != null ? batchType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BatchExecution{" +
                "batchQueries=" + batchQueries +
                ", consistency='" + consistency + '\'' +
                ", batchType=" + batchType +
                '}';
    }

    public static BatchExecutionBuilder builder() {
        return new BatchExecutionBuilder();
    }


    public static class BatchExecutionBuilder {
        private List<BatchQuery> batchQueries;
        private String consistency;
        private BatchType batchType;

        private BatchExecutionBuilder() {
        }

        public BatchExecutionBuilder withBatchQueries(List<BatchQuery> batchQueries) {
            this.batchQueries = batchQueries;
            return this;
        }

        public BatchExecutionBuilder withBatchQueries(BatchQuery... batchQueries) {
            this.batchQueries = Arrays.asList(batchQueries);
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
            return new BatchExecution(batchQueries, consistency, batchType);
        }
    }
}
