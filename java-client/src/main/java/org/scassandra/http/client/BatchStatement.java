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
}
