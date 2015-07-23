package common;

public class CassandraQuery {
    private final String query;

    public CassandraQuery(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}
