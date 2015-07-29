package org.scassandra.http.client;

public final class BatchQuery {
    private final String query;

    private BatchQuery(String query) {
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

        BatchQuery that = (BatchQuery) o;

        return !(query != null ? !query.equals(that.query) : that.query != null);

    }

    @Override
    public int hashCode() {
        return query != null ? query.hashCode() : 0;
    }


    public static BatchQueryBuilder builder() {
        return new BatchQueryBuilder();
    }

    public static class BatchQueryBuilder {
        private String query;

        private BatchQueryBuilder() {
        }

        public BatchQueryBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public BatchQuery build() {
            return new BatchQuery(query);
        }
    }
}
