package batches;

import cassandra.CassandraExecutor30;

public class BatchActivityVerificationTest30 extends BatchActivityVerificationTest {
    public BatchActivityVerificationTest30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }
}
