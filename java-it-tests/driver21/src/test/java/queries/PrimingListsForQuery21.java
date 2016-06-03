package queries;

import cassandra.CassandraExecutor21;

public class PrimingListsForQuery21 extends PrimingListsForQuery {
    public PrimingListsForQuery21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
