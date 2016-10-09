package org.scassandra.cql;

import java.math.BigInteger;

public class CqlVarint extends PrimitiveType {
    CqlVarint() {
        super("varint");
    }
    @Override
    public boolean equals(Object expected, Object actual) {
        if (expected == null) return actual == null;
        if (actual == null) return expected == null;

        Long typedActual = getActualValueLong(actual);

        if (expected instanceof BigInteger) {
            return expected.equals(new BigInteger(typedActual.toString()));
        } else if (expected instanceof String) {
            try {
                return new BigInteger((String) expected).equals(new BigInteger(typedActual.toString()));
            } catch (NumberFormatException e) {
                throw throwInvalidType(expected, actual, this);
            }
        } else {
            throw throwInvalidType(expected, actual, this);
        }
    }
}
