package queries;

import cassandra.CassandraExecutor21;

public class DelayTest21 extends DelayTest {
    public DelayTest21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
