package org.scassandra.cql;

public class CqlDouble extends PrimitiveType {
    CqlDouble() {
        super("double");
    }
    @Override
    public boolean equals(Object expected, Object actual) {
        return equalsDecimalType(expected, actual, this);

    }
}
