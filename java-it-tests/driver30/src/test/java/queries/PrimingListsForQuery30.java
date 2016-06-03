package queries;

import cassandra.CassandraExecutor30;

public class PrimingListsForQuery30 extends PrimingListsForQuery {
    public PrimingListsForQuery30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }
}
