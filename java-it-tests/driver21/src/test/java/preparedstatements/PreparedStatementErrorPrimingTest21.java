package preparedstatements;

import cassandra.CassandraExecutor21;

public class PreparedStatementErrorPrimingTest21 extends PreparedStatementErrorPrimingTest {

    public PreparedStatementErrorPrimingTest21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
