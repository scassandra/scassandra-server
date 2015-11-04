package preparedstatements;

import cassandra.CassandraExecutor30;

public class PreparedStatementErrorPrimingTest30 extends PreparedStatementErrorPrimingTest {

    public PreparedStatementErrorPrimingTest30() {
        super(new CassandraExecutor30());
    }
}
