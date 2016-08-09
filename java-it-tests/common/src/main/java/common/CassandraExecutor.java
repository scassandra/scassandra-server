/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
