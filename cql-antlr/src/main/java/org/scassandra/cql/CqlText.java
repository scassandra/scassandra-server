package org.scassandra.cql;

public class CqlText extends PrimitiveType {
    CqlText() {
        super("text");
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        return ASCII.equals(expected, actual);
    }
}
