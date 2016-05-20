package preparedstatements;

import cassandra.CassandraExecutor20;

public class PreparedStatementsWithPattern20 extends PreparedStatementsWithPattern {

    public PreparedStatementsWithPattern20() {
        super(new CassandraExecutor20(scassandra.getBinaryPort()));
    }
}
