package preparedstatements;

import cassandra.CassandraExecutor21;

public class PreparedStatementDelayTest21 extends PreparedStatementDelayTest {
    public PreparedStatementDelayTest21() {
        super(new CassandraExecutor21());
    }
}
