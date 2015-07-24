package queries;

import cassandra.CassandraExecutor10;

public class PrimingSetsForQuery10 extends PrimingSetsForQuery {
    public PrimingSetsForQuery10() {
        super(new CassandraExecutor10());
    }
}
