package org.scassandra.cql;

public class CqlFloat extends PrimitiveType {
    CqlFloat() {
        super("float");
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        return equalsDecimalType(expected, actual, this);

    }
}
