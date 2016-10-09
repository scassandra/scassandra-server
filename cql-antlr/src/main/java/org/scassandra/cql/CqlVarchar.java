package org.scassandra.cql;

public class CqlVarchar extends PrimitiveType {
    CqlVarchar() {
        super("varchar");
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        return ASCII.equals(expected, actual);
    }
}
