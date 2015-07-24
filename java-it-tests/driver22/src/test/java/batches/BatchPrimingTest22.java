package batches;

import cassandra.CassandraExecutor22;

public class BatchPrimingTest22 extends BatchPrimingTest {
    public BatchPrimingTest22() {
        super(new CassandraExecutor22());
    }
}
