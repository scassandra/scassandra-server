package queries;

import cassandra.CassandraExecutor10;

public class PrimingWithPattern10 extends PrimingWithPattern {

    public PrimingWithPattern10() {
        super(new CassandraExecutor10());
    }
}
