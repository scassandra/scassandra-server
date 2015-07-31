package preparedstatements;

import common.AbstractScassandraTest;
import common.CassandraExecutor;
import org.junit.Test;
import org.scassandra.http.client.PreparedStatementPreparation;

import java.util.List;

import static org.junit.Assert.assertEquals;

abstract public class PreparedStatementPreparationTest extends AbstractScassandraTest {

    public PreparedStatementPreparationTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void activityVerificationForPreparedPrimesThatAreExecuted() {
        //given
        String preparedStatementText = "select * from people where name = ?";

        //when
        cassandra().prepareAndExecute(preparedStatementText, "Chris");

        //then
        List<PreparedStatementPreparation> preparations = activityClient.retrievePreparedStatementPreparations();
        assertEquals(1, preparations.size());
        PreparedStatementPreparation preparation = preparations.get(0);
        assertEquals(preparedStatementText, preparation.getPreparedStatementText());
    }


    @Test
    public void activityVerificationForPreparedPrimesThatAreNotExecuted() {
        //given
        String preparedStatementText = "select * from people where name = ?";

        //when
        cassandra().prepare(preparedStatementText);

        //then
        List<PreparedStatementPreparation> preparations = activityClient.retrievePreparedStatementPreparations();
        assertEquals(1, preparations.size());
        PreparedStatementPreparation preparation = preparations.get(0);
        assertEquals(preparedStatementText, preparation.getPreparedStatementText());
    }

    @Test
    public void clearingRecordedActivityForPreparedPrimes() {
        //given
        String preparedStatementText = "select * from people where name = ?";

        //when
        cassandra().prepare(preparedStatementText);

        //then
        activityClient.clearPreparedStatementPreparations();
        List<PreparedStatementPreparation> preparations = activityClient.retrievePreparedStatementPreparations();
        assertEquals(0, preparations.size());
    }

}
