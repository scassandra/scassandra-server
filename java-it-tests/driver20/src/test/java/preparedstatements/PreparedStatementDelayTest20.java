package preparedstatements;

import cassandra.CassandraExecutor20;

public class PreparedStatementDelayTest20 extends PreparedStatementDelayTest {
    public PreparedStatementDelayTest20() {
        super(new CassandraExecutor20(scassandra.getBinaryPort()));
    }
}
