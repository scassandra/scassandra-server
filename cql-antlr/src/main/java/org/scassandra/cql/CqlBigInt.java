package org.scassandra.cql;

public class CqlBigInt extends PrimitiveType {
    CqlBigInt() {
        super("bigint");
    }
    @Override
    public boolean equals(Object expected, Object actual) {
        return equalsForLongType(expected, actual, this);
    }
}
