package queries;


import cassandra.CassandraExecutor30;

public class PrimingCollectionsForQuery30 extends PrimingMapsForQuery {
    public PrimingCollectionsForQuery30() {
        super(new CassandraExecutor30());
    }
}
