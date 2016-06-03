package preparedstatements;

import cassandra.CassandraExecutor30;

public class PreparedStatementsListVariablesWithMatcher30 extends PreparedStatementsListVariablesWithMatcher {
    public PreparedStatementsListVariablesWithMatcher30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }
}
