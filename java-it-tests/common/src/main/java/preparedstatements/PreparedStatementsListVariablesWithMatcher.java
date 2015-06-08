package preparedstatements;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import org.junit.Test;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PreparedStatementExecution;
import org.scassandra.http.client.PrimingRequest;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.scassandra.matchers.Matchers.preparedStatementRecorded;

abstract public class PreparedStatementsListVariablesWithMatcher extends AbstractScassandraTest {

    public PreparedStatementsListVariablesWithMatcher(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void textLists() {
        List<String> listValue = Lists.newArrayList("one", "two", "three");
        Map<String, ? extends Object> rows = ImmutableMap.of("list_field", "hello");
        String query = "insert into set_table (id, textlist ) values (?, ?)";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Int, ColumnTypes.TextList)
                .withRows(rows)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, 1, listValue);

        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withConsistency("ONE")
                .withPreparedStatementText(query)
                .withVariables(1, listValue)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedExecution));
    }

    @Test
    public void textListThatDoesNotMatch() {
        List<String> listValue = Lists.newArrayList("one", "two", "three");
        Map<String, ? extends Object> rows = ImmutableMap.of("list_field", "hello");
        String query = "insert into set_table (id, textlist ) values (?, ?)";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Int, ColumnTypes.TextList)
                .withRows(rows)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, 1, listValue);

        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withConsistency("ONE")
                .withPreparedStatementText(query)
                .withVariables(1, Lists.newArrayList("i am not the list you are looking for"))
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), not(preparedStatementRecorded(expectedExecution)));
    }
}
