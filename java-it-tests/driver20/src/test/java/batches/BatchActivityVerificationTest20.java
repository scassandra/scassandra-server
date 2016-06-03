package batches;

import cassandra.CassandraExecutor20;

public class BatchActivityVerificationTest20 extends BatchActivityVerificationTest {
    public BatchActivityVerificationTest20() {
        super(new CassandraExecutor20(scassandra.getBinaryPort()));
    }
}
