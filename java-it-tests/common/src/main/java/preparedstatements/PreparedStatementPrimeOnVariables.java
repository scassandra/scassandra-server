package preparedstatements;

import com.google.common.collect.ImmutableMap;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import org.junit.Test;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PreparedStatementExecution;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.types.ColumnMetadata;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.http.client.PrimingRequest.then;


abstract public class PreparedStatementPrimeOnVariables extends AbstractScassandraTest {

    public PreparedStatementPrimeOnVariables(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testUUIDasVariableTypeAndInRow() {
        Map<String, String> rows = ImmutableMap.of("field", "");
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("col", TEXT))
                        .withVariableTypes(TEXT)
                )
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, "blah");


    }

}
