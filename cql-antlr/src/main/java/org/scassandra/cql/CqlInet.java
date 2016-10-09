package org.scassandra.cql;

import java.net.InetAddress;

public class CqlInet extends PrimitiveType {
    CqlInet() {
        super("inet");
    }

    // comes from the server as a string
    @Override
    public boolean equals(Object expected, Object actual) {
        if (expected == null) return actual == null;
        if (actual == null) return expected == null;

        if (expected instanceof String) {
            try {
                return expected.equals(actual);
            } catch (Exception e) {
                throw throwInvalidType(expected, actual, this);
            }
        } else if (expected instanceof InetAddress) {
            return ((InetAddress) expected).getHostAddress().equals(actual);
        }

        throw throwInvalidType(expected, actual, this);
    }
}
