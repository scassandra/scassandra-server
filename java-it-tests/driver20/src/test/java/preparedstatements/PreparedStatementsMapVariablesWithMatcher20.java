package preparedstatements;

import cassandra.CassandraExecutor20;

public class PreparedStatementsMapVariablesWithMatcher20 extends PreparedStatementsMapVariablesWithMatcher {
    public PreparedStatementsMapVariablesWithMatcher20() {
        super(new CassandraExecutor20(scassandra.getBinaryPort()));
    }
}
