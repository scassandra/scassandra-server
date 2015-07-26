package batches;

import cassandra.CassandraExecutor21;

public class BatchActivityVerificationTest21 extends BatchActivityVerificationTest {
    public BatchActivityVerificationTest21() {
        super(new CassandraExecutor21());
    }
}
