package queries;

import cassandra.CassandraExecutor10;

public class PrimingListsForQuery10 extends PrimingListsForQuery {
    public PrimingListsForQuery10() {
        super(new CassandraExecutor10());
    }
}
