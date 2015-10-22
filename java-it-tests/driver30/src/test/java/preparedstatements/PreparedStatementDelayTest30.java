package preparedstatements;

import cassandra.CassandraExecutor30;

public class PreparedStatementDelayTest30 extends PreparedStatementDelayTest {
    public PreparedStatementDelayTest30() {
        super(new CassandraExecutor30());
    }
}
