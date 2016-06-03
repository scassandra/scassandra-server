package preparedstatements;

import cassandra.CassandraExecutor30;

public class PreparedStatementsWithPattern30 extends PreparedStatementsWithPattern {

    public PreparedStatementsWithPattern30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }
}
