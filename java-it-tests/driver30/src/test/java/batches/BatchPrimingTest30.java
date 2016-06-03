package batches;

import cassandra.CassandraExecutor30;

public class BatchPrimingTest30 extends BatchPrimingTest {
    public BatchPrimingTest30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }
}
