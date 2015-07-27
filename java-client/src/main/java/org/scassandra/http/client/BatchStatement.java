package org.scassandra.http.client;

public final class BatchStatement {
    private final String query;

    private BatchStatement(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public String toString() {
        return "BatchStatement{" +
                "query='" + query + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BatchStatement that = (BatchStatement) o;

        return !(query != null ? !query.equals(that.query) : that.query != null);

    }

    @Override
    public int hashCode() {
        return query != null ? query.hashCode() : 0;
    }


    public static BatchStatementBuilder builder() {
        return new BatchStatementBuilder();
    }

    public static class BatchStatementBuilder {
        private String query;

        private BatchStatementBuilder() {
        }

        public BatchStatementBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public BatchStatement build() {
            return new BatchStatement(query);
        }
    }
}
