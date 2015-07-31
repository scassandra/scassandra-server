package preparedstatements;

import cassandra.CassandraExecutor21;

public class PreparedStatementPreparationTest21 extends PreparedStatementPreparationTest {

    public PreparedStatementPreparationTest21() {
        super(new CassandraExecutor21());
    }

}
