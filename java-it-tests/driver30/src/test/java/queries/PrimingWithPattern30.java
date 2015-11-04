package queries;

import cassandra.CassandraExecutor30;

public class PrimingWithPattern30 extends PrimingWithPattern {

    public PrimingWithPattern30() {
        super(new CassandraExecutor30());
    }
}
