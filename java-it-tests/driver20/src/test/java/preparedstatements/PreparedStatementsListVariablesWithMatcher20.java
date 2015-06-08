package preparedstatements;

import cassandra.CassandraExecutor20;

public class PreparedStatementsListVariablesWithMatcher20 extends PreparedStatementsListVariablesWithMatcher {
    public PreparedStatementsListVariablesWithMatcher20() {
        super(new CassandraExecutor20());
    }
}
