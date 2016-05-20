package preparedstatements;

import cassandra.CassandraExecutor21;

public class PreparedStatementsWithPattern21 extends PreparedStatementsWithPattern {

    public PreparedStatementsWithPattern21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
