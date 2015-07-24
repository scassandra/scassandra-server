package batches;

import cassandra.CassandraExecutor21;

public class BatchPrimingTest21 extends BatchPrimingTest {
    public BatchPrimingTest21() {
        super(new CassandraExecutor21());
    }
}
