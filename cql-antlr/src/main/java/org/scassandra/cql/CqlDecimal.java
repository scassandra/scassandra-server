package org.scassandra.cql;

public class CqlDecimal extends PrimitiveType {
    CqlDecimal() {
        super("decimal");
    }
    @Override
    public boolean equals(Object expected, Object actual) {
        return equalsDecimalType(expected, actual, this);

    }
}
