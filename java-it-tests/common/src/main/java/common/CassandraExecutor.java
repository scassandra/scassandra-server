package common;

import com.google.common.base.Optional;
import org.scassandra.http.client.BatchType;

import java.util.List;
import java.util.UUID;

public interface CassandraExecutor {
    CassandraResult executeQuery(String query);

    CassandraResult executeSimpleStatement(String query, String consistency);

    void prepare(String preparedStatementText);

    CassandraResult prepareAndExecute(String query, Object... variable);

    CassandraResult prepareAndExecuteWithConsistency(String query, String consistency, Object... vars);

    CassandraResult executeSelectWithBuilder(String table, Optional<WhereEquals> clause);

    CassandraResult executeSelectWithBuilder(String table);

    CassandraResult executeBatch(List<CassandraQuery> queries, BatchType type);

    UUID currentTimeUUID();

    void close();

}
