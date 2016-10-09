package org.scassandra.cql;

public class CqlUuid extends PrimitiveType {
    CqlUuid() {
        super("uuid");
    }

    // comes back from the server as a string
    @Override
    public boolean equals(Object expected, Object actual) {
        return equalsForUUID(expected, actual, this);
    }
}
