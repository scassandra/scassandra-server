package common;

import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.cql.CqlType;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PrimingClient;

abstract public class AbstractScassandraTest {
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

    public static Map<String, CqlType> cqlTypes(Map<String, ColumnTypes> columnTypes) {
        return Maps.transformValues(columnTypes, ColumnTypes::getType);
    }

    public static byte[] getArray(ByteBuffer buffer) {
        int length = buffer.remaining();
        byte[] data = new byte[length];
        buffer.get(data);
        return data;
    }

}
