package org.scassandra.cql;

import java.util.Date;

public class CqlTimestamp extends PrimitiveType {
    CqlTimestamp() {
        super("timestamp");
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        Long typedActualValue = getActualValueLong(actual);

        if (expected == null) return actual == null;
        if (actual == null) return expected == null;

        if (expected instanceof Long) {
            return expected.equals(typedActualValue);
        } else if (expected instanceof Date) {
            return ((Date) expected).getTime() == typedActualValue;
        }

        throw throwInvalidType(expected, actual, this);

    }
}
