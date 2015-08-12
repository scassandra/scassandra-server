package org.scassandra.http.client;

public final class BatchQuery {

    private final String query;
    private final BatchQueryKind batchQueryKind;

    private BatchQuery(String query, BatchQueryKind batchQueryKind) {
        this.query = query;
        this.batchQueryKind = batchQueryKind;
    }

    public String getQuery() {
        return query;
    }

    public BatchQueryKind getBatchQueryKind() {
        return batchQueryKind;
    }

    @Override
    public String toString() {
        return "BatchQuery{" +
                "query='" + query + '\'' +
                ", batchQueryType=" + batchQueryKind +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BatchQuery that = (BatchQuery) o;

        if (query != null ? !query.equals(that.query) : that.query != null) return false;
        return batchQueryKind == that.batchQueryKind;

    }

    @Override
    public int hashCode() {
        int result = query != null ? query.hashCode() : 0;
        result = 31 * result + (batchQueryKind != null ? batchQueryKind.hashCode() : 0);
        return result;
    }

    public static BatchQueryBuilder builder() {
        return new BatchQueryBuilder();
    }

    public static class BatchQueryBuilder {
        private String query;
        private BatchQueryKind batchQueryKind = BatchQueryKind.query;

        private BatchQueryBuilder() {
        }

        public BatchQueryBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public BatchQueryBuilder withType(BatchQueryKind type) {
            this.batchQueryKind = type;
            return this;
        }

        public BatchQuery build() {
            return new BatchQuery(query, batchQueryKind);
        }
    }
}
