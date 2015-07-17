package cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * A Custom {@link RetryPolicy} to rethrow on unavailable instead of retrying
 * on another host which the java-driver does as of 2.0.11/2.1.7 (JAVA-709).
 */
public class NoRetryOnUnavailablePolicy implements RetryPolicy {

    public static final NoRetryOnUnavailablePolicy INSTANCE = new NoRetryOnUnavailablePolicy();

    private NoRetryOnUnavailablePolicy() {}

    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        return receivedResponses >= requiredResponses && !dataRetrieved ? RetryDecision.retry(cl) : RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();

        // If the batch log write failed, retry the operation as this might just be we were unlucky at picking candidates
        return writeType == WriteType.BATCH_LOG ? RetryDecision.retry(cl) : RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        // Overrides default behavior of trying next host
        return RetryDecision.rethrow();
    }

    @Override
    public void init(Cluster cluster) {}

    @Override
    public void close() {}
}
