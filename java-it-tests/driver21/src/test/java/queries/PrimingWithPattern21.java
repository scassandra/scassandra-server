package queries;

import cassandra.CassandraExecutor21;

public class PrimingWithPattern21 extends PrimingWithPattern {

    public PrimingWithPattern21() {
        super(new CassandraExecutor21(scassandra.getBinaryPort()));
    }
}
