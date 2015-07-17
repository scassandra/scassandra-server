package queries;

import cassandra.CassandraExecutor22;

public class DelayTest22 extends DelayTest {
    public DelayTest22() {
        super(new CassandraExecutor22());
    }
}
