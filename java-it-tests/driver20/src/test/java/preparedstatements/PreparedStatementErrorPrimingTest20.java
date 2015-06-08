package preparedstatements;

import cassandra.CassandraExecutor20;

public class PreparedStatementErrorPrimingTest20 extends PreparedStatementErrorPrimingTest {

    public PreparedStatementErrorPrimingTest20() {
        super(new CassandraExecutor20());
    }
}
