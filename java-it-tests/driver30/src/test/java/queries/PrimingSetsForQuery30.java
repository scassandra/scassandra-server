package queries;

import cassandra.CassandraExecutor30;

public class PrimingSetsForQuery30 extends PrimingSetsForQuery {
    public PrimingSetsForQuery30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }
}
