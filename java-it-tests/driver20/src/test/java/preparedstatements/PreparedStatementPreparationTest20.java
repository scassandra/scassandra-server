package preparedstatements;

import cassandra.CassandraExecutor20;

public class PreparedStatementPreparationTest20 extends PreparedStatementPreparationTest {
    public PreparedStatementPreparationTest20() {
        super(new CassandraExecutor20());
    }
}
