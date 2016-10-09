package org.scassandra.cql;

public class CqlInt extends PrimitiveType {
    CqlInt() {
        super("int");
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        return equalsForLongType(expected, actual, this);
    }
}
