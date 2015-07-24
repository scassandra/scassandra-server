package queries;


import cassandra.CassandraExecutor10;

public class PrimingCollectionsForQuery10 extends PrimingMapsForQuery {
    public PrimingCollectionsForQuery10() {
        super(new CassandraExecutor10());
    }
}
