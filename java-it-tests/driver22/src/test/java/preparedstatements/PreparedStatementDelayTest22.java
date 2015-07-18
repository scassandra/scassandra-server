package preparedstatements;

import cassandra.CassandraExecutor22;

public class PreparedStatementDelayTest22 extends PreparedStatementDelayTest {
    public PreparedStatementDelayTest22() {
        super(new CassandraExecutor22());
    }
}
