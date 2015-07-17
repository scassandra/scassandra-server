package queries;


import cassandra.CassandraExecutor22;

public class PrimingCollectionsForQuery22 extends PrimingMapsForQuery {
    public PrimingCollectionsForQuery22() {
        super(new CassandraExecutor22());
    }
}
