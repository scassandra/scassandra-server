package org.scassandra.http.client;

public final class BatchStatement {
    private final String query;

    public BatchStatement(String query) {
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
}
