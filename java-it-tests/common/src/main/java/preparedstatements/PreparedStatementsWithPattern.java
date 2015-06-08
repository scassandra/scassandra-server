package preparedstatements;

import com.google.common.collect.ImmutableMap;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraResult;
import common.CassandraRow;
import org.junit.Test;
import org.scassandra.http.client.PrimingRequest;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

abstract public class PreparedStatementsWithPattern extends AbstractScassandraTest {

    public PreparedStatementsWithPattern(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void primingWithADotStar() throws Exception {
        //given
        Map<String, String> row = ImmutableMap.of("name", "Chris");
        primingClient.primePreparedStatement(PrimingRequest.preparedStatementBuilder()
                .withQueryPattern(".*")
                .withRows(row)
                .build());

        //when
        CassandraResult results = cassandra().prepareAndExecute("select * from people where name = ?", "Chris");

        //then
        List<CassandraRow> asList = results.rows();
        assertEquals(1, asList.size());
        assertEquals("Chris", asList.get(0).getString("name"));
    }

}
