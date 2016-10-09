package org.scassandra.cql;

public class CqlAscii extends PrimitiveType {
    CqlAscii() {
        super("ascii");
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        if (expected == null) return actual == null;
        return expected.equals(actual);
    }
}
