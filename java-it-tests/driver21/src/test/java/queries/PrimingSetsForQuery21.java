package queries;

import cassandra.CassandraExecutor21;

public class PrimingSetsForQuery21 extends PrimingSetsForQuery {
    public PrimingSetsForQuery21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
