package queries;

import cassandra.CassandraExecutor30;

public class DelayTest30 extends DelayTest {
    public DelayTest30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }
}
