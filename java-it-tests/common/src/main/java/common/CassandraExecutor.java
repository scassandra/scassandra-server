package common;

import com.google.common.base.Optional;

import java.util.List;

public interface CassandraExecutor {
    CassandraResult executeQuery(String query);

    CassandraResult executeSimpleStatement(String query, String consistency);

    CassandraResult prepareAndExecute(String query, Object... variable);

    CassandraResult prepareAndExecuteWithConsistency(String query, String consistency, Object... vars);

    CassandraResult executeSelectWithBuilder(String table, Optional<WhereEquals> clause);

    CassandraResult executeSelectWithBuilder(String table);

    CassandraResult executeBatch(List<CassandraQuery> queries);

    void close();

}
