package batches;

import cassandra.CassandraExecutor22;

public class BatchActivityVerificationTest22 extends BatchActivityVerificationTest {
    public BatchActivityVerificationTest22() {
        super(new CassandraExecutor22());
    }
}
