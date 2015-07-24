package preparedstatements;

import cassandra.CassandraExecutor10;

public class PreparedStatementErrorPrimingTest10 extends PreparedStatementErrorPrimingTest {

    public PreparedStatementErrorPrimingTest10() {
        super(new CassandraExecutor10());
    }
}
