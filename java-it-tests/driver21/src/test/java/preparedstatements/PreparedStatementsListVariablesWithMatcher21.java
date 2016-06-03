package preparedstatements;

import cassandra.CassandraExecutor21;

public class PreparedStatementsListVariablesWithMatcher21 extends PreparedStatementsListVariablesWithMatcher {
    public PreparedStatementsListVariablesWithMatcher21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
