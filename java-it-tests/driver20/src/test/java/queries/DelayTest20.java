package queries;

import cassandra.CassandraExecutor20;

public class DelayTest20 extends DelayTest {
    public DelayTest20() {
        super(new CassandraExecutor20(scassandra.getBinaryPort()));
    }
}
