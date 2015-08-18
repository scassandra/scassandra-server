package cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Optional;
import common.*;
import org.scassandra.http.client.BatchType;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.WriteTypePrime;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Function;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static common.Config.KEYSPACE;
import static org.scassandra.http.client.PrimingRequest.Result.*;
import static org.scassandra.http.client.PrimingRequest.Result.already_exists;

public class CassandraExecutor21 implements CassandraExecutor {
    private Cluster cluster;
    private Session session;

    public CassandraExecutor21() {
        cluster = Cluster.builder().addContactPoint(Config.NATIVE_HOST).withPort(Config.NATIVE_PORT)
                .withRetryPolicy(NoRetryOnUnavailablePolicy.INSTANCE).build();
        session = cluster.connect(KEYSPACE);
    }

    @Override
    public CassandraResult executeQuery(String query) {
        return execute(session::execute, query);
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
        return new CassandraResult21(session.execute(bind));
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
        return new CassandraResult21(session.execute(query));
    }

    @Override
    public CassandraResult executeSelectWithBuilder(String table) {
        return executeSelectWithBuilder(table, Optional.<WhereEquals>absent());
    }

    @Override
    public CassandraResult executeBatch(List<CassandraQuery> queries, BatchType batchType) {
        BatchStatement batch = new BatchStatement(BatchStatement.Type.valueOf(batchType.name()));
        queries.forEach(query -> {
            switch (query.getQueryType()) {
                case QUERY:
                    batch.add(new SimpleStatement(query.getQuery(), query.getVariables()));
                    break;
                case PREPARED_STATEMENT:
                    batch.add(session.prepare(query.getQuery()).bind(query.getVariables()));
                    break;
            }
        });
        return execute(session::execute, batch);
    }

    @Override
    public void close() {
        cluster.close();
    }


    private <A> CassandraResult21 execute(Function<A, ResultSet> function, A input) {
        ResultSet resultSet;
        try {
            resultSet = function.apply(input);
        } catch (ReadTimeoutException e) {
            return new CassandraResult21(new CassandraResult.ReadTimeoutStatus(
                    e.getConsistencyLevel().toString(),
                    e.getReceivedAcknowledgements(),
                    e.getRequiredAcknowledgements(),
                    e.wasDataRetrieved()));
        } catch (WriteTimeoutException e) {
            return new CassandraResult21(
                    new CassandraResult.WriteTimeoutStatus(e.getConsistencyLevel().toString(),
                            e.getReceivedAcknowledgements(),
                            e.getRequiredAcknowledgements(),
                            WriteTypePrime.valueOf(e.getWriteType().toString())));
        } catch (UnavailableException e) {
            return new CassandraResult21(
                    new CassandraResult.UnavailableStatus(e.getConsistencyLevel().toString(),
                            e.getRequiredReplicas(),
                            e.getAliveReplicas()));
        } catch (NoHostAvailableException e) {
            PrimingRequest.Result error = server_error;
            String message = "";
            InetSocketAddress addr = e.getErrors().keySet().iterator().next();
            Throwable e1 = e.getErrors().get(addr);
            try {
                throw e1;
            } catch(DriverException de) {
                message = de.getMessage();
                // These errors are thrown in NHAE as the driver considers them host-specific errors and
                // tries another host.
                if(message.contains("protocol error")) {
                    error = protocol_error;
                } else if(message.contains("Host overloaded")) {
                    error = overloaded;
                } else if(message.contains("Host is bootstrapping")) {
                    error = is_bootstrapping;
                }
            } catch(Throwable t) {} // unknown error we can handle later.
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(error, message));
        } catch(DriverInternalError e) {
            PrimingRequest.Result error = protocol_error;
            String message = e.getMessage();
            // Unprepared is thrown as a DriverInternalError if the Driver doesn't know about the query either
            // as this is unexpected behavior.
            if(message.startsWith("Tried to execute unknown prepared query")) {
                error = unprepared;
            }
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(error, message));
        } catch(AuthenticationException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(bad_credentials, e.getMessage()));
        } catch(TruncateException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(truncate_error, e.getMessage()));
        } catch(SyntaxError e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(syntax_error, e.getMessage()));
        } catch(UnauthorizedException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(unauthorized, e.getMessage()));
        } catch(InvalidConfigurationInQueryException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(config_error, e.getMessage()));
        } catch(InvalidQueryException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(invalid, e.getMessage()));
        } catch(AlreadyExistsException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(already_exists, e.getMessage()));
        }
        return new CassandraResult21(resultSet);
    }
}
