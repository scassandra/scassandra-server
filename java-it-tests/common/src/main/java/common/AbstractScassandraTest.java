package common;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;

abstract public class AbstractScassandraTest {
    protected static int binaryPort = 8042;
    private static Scassandra scassandra;
    protected static PrimingClient primingClient;
    protected static ActivityClient activityClient;

    private CassandraExecutor cassandraExecutor;

    public AbstractScassandraTest(CassandraExecutor cassandraExecutor) {
        this.cassandraExecutor = cassandraExecutor;
    }


    @BeforeClass
    public static void startScassandra() {
        scassandra = ScassandraFactory.createServer();
        primingClient = scassandra.primingClient();
        activityClient = scassandra.activityClient();
        scassandra.start();
    }

    @AfterClass
    public static void stopScassandra() {
        scassandra.stop();
    }

    @Before
    public void setup() {
        activityClient.clearAllRecordedActivity();
        primingClient.clearAllPrimes();
    }

    @After
    public void shutdownCluster() {
        cassandraExecutor.close();
    }

    public CassandraExecutor cassandra() {
        return cassandraExecutor;
    }


}
