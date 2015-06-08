package queries;

import cassandra.CassandraExecutor20;

public class PrimingSetsForQuery20 extends PrimingSetsForQuery {
    public PrimingSetsForQuery20() {
        super(new CassandraExecutor20());
    }
}
