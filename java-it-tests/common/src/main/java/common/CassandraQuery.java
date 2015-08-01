package common;

public class CassandraQuery {
    private final String query;
    private final QueryType queryType;
    private final Object[] variables;

    public CassandraQuery(String query) {
        this(query, new Object[] {});
    }

    public CassandraQuery(String query, Object... variables) {
        this(query, QueryType.QUERY, variables);
    }

    public CassandraQuery(String query, QueryType queryType, Object... variables) {
        this.query = query;
        this.queryType = queryType;
        this.variables = variables;
    }

    public String getQuery() {
        return query;
    }

    public Object[] getVariables() {
        return variables;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public enum QueryType {
        QUERY, PREPARED_STATEMENT
    }
}
