package org.scassandra.cql;

public class CqlBoolean extends PrimitiveType {
    CqlBoolean() {
        super("boolean");
    }
    @Override
    public boolean equals(Object expected, Object actual) {
        if (expected == null) throw throwNullError(actual, this);

        Boolean actualTyped;
        if (actual instanceof Boolean) {
            actualTyped = (Boolean) actual;
        } else {
            actualTyped = Boolean.parseBoolean(actual.toString());
        }

        if (expected instanceof Boolean) {
            return expected.equals(actualTyped);
        } else {
            throw throwInvalidType(expected, actual, this);
        }
    }
}
