package queries;

import cassandra.CassandraExecutor20;

public class PrimingListsForQuery20 extends PrimingListsForQuery {
    public PrimingListsForQuery20() {
        super(new CassandraExecutor20());
    }
}
