package batches;

import common.AbstractScassandraTest;
import common.CassandraExecutor;

public abstract class BatchPrimingErrorsTest extends AbstractScassandraTest {
    public BatchPrimingErrorsTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    //todo prime batch log error
    //todo prime normal error
}
