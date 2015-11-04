package preparedstatements;

import cassandra.CassandraExecutor30;

public class PreparedStatementPreparationTest30 extends PreparedStatementPreparationTest {

    public PreparedStatementPreparationTest30() {
        super(new CassandraExecutor30());
    }

}
