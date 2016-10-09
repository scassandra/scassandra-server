package org.scassandra.cql;

public class CqlCounter extends PrimitiveType {
    CqlCounter() {
        super("counter");
    }
    @Override
    public boolean equals(Object expected, Object actual) {
        return equalsForLongType(expected, actual, this);
    }
}
