package common;

public class CassandraQuery {
    private final String query;
    private final Object[] variables;

    public CassandraQuery(String query) {
        this(query, new Object[] {});
    }

    public CassandraQuery(String query, Object... variables) {
        this.query = query;
        this.variables = variables;
    }

    public String getQuery() {
        return query;
    }

    public Object[] getVariables() {
        return variables;
    }
}
