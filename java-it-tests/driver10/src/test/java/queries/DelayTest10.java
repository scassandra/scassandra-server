package queries;

import cassandra.CassandraExecutor10;

public class DelayTest10 extends DelayTest {
    public DelayTest10() {
        super(new CassandraExecutor10());
    }
}
