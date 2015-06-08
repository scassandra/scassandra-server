package queries;


import cassandra.CassandraExecutor21;

public class PrimingCollectionsForQuery21 extends PrimingMapsForQuery {
    public PrimingCollectionsForQuery21() {
        super(new CassandraExecutor21());
    }
}
