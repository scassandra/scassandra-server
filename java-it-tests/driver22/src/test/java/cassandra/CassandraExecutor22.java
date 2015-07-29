package cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Optional;
import common.*;
import org.scassandra.http.client.WriteTypePrime;

import java.util.List;
import java.util.function.Function;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class CassandraExecutor22 implements CassandraExecutor {
    private Cluster cluster;
    private Session session;

    public CassandraExecutor22() {
        cluster = Cluster.builder().addContactPoint(Config.NATIVE_HOST).withPort(Config.NATIVE_PORT).withRetryPolicy(
            NoRetryOnUnavailablePolicy.INSTANCE).build();
        session = cluster.connect(Config.KEYSPACE);
    }

    @Override
    public CassandraResult executeQuery(String query) {
        return this.execute(session::execute, query);
    }

    @Override
    public CassandraResult executeSimpleStatement(String query, String consistency) {
        SimpleStatement simpleStatement = new SimpleStatement(query);
        Statement statement = simpleStatement.setConsistencyLevel(ConsistencyLevel.valueOf(consistency));
        return this.execute(session::execute, statement);
    }

    @Override
    public void prepare(String query) {
        session.prepare(query);
    }

    @Override
    public CassandraResult prepareAndExecute(String query, Object... variable) {
        PreparedStatement prepare = session.prepare(query);
        BoundStatement bind = prepare.bind(variable);
        return new CassandraResult22(session.execute(bind));
    }

    @Override
    public CassandraResult prepareAndExecuteWithConsistency(String query, String consistency, Object... vars) {
        PreparedStatement prepare = session.prepare(query);
        prepare.setConsistencyLevel(ConsistencyLevel.valueOf(consistency));
        BoundStatement bind = prepare.bind(vars);
        return this.execute(session::execute, bind);
    }

    @Override
    public CassandraResult executeSelectWithBuilder(String table, Optional<WhereEquals> clause) {

        Select query = QueryBuilder.select().all().from(table);
        if (clause.isPresent()) {
            query.where(eq(clause.get().getField(), clause.get().getValue()));
        }
        return new CassandraResult22(session.execute(query));
    }

    @Override
    public CassandraResult executeSelectWithBuilder(String table) {
        return this.executeSelectWithBuilder(table, Optional.<WhereEquals>absent());
    }

    @Override
    public CassandraResult executeBatch(List<CassandraQuery> queries) {
        BatchStatement batch = new BatchStatement();
        queries.forEach(query -> {
            batch.add(new SimpleStatement(query.getQuery()));
        });
        return new CassandraResult22(session.execute(batch));
    }

    @Override
    public void close() {
        cluster.close();
    }


    private <A> CassandraResult22 execute(Function<A, ResultSet> function, A input) {
        ResultSet resultSet;
        try {
            resultSet = function.apply(input);
        } catch (ReadTimeoutException e) {
            return new CassandraResult22(new CassandraResult.ReadTimeoutStatus(
                    e.getConsistencyLevel().toString(),
                    e.getReceivedAcknowledgements(),
                    e.getRequiredAcknowledgements(),
                    e.wasDataRetrieved()));
        } catch (WriteTimeoutException e) {
            return new CassandraResult22(
                    new CassandraResult.WriteTimeoutStatus(e.getConsistencyLevel().toString(),
                    e.getReceivedAcknowledgements(),
                    e.getRequiredAcknowledgements(),
                    WriteTypePrime.valueOf(e.getWriteType().toString())));
        } catch (UnavailableException e) {
            return new CassandraResult22(
                    new CassandraResult.UnavailableStatus(e.getConsistencyLevel().toString(),
                    e.getRequiredReplicas(),
                    e.getAliveReplicas()));
        }
        return new CassandraResult22(resultSet);
    }
}
