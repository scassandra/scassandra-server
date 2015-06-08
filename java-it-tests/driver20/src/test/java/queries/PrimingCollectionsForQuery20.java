package queries;


import cassandra.CassandraExecutor20;

public class PrimingCollectionsForQuery20 extends PrimingMapsForQuery {
    public PrimingCollectionsForQuery20() {
        super(new CassandraExecutor20());
    }
}
