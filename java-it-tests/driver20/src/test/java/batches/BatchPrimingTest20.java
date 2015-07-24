package batches;

import cassandra.CassandraExecutor20;

public class BatchPrimingTest20 extends BatchPrimingTest {
    public BatchPrimingTest20() {
        super(new CassandraExecutor20());
    }
}
