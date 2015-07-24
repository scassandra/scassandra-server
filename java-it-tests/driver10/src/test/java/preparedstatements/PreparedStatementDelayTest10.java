package preparedstatements;

import cassandra.CassandraExecutor10;

public class PreparedStatementDelayTest10 extends PreparedStatementDelayTest {
    public PreparedStatementDelayTest10() {
        super(new CassandraExecutor10());
    }
}
