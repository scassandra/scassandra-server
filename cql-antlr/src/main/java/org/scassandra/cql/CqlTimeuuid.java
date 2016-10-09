package org.scassandra.cql;

public class CqlTimeuuid extends PrimitiveType {
    CqlTimeuuid() {
        super("timeuuid");
    }

    // comes back from the server as a string
    @Override
    public boolean equals(Object expected, Object actual) {
        return equalsForUUID(expected, actual, this);
    }

}
