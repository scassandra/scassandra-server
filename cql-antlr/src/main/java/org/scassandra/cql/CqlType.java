package org.scassandra.cql;

abstract public class CqlType {
    public abstract String serialise();
    public abstract boolean equals(Object expected, Object actual);

    protected IllegalArgumentException throwInvalidType(Object expected, Object actual, CqlType instance) {
        return new IllegalArgumentException(String.format("Invalid expected value (%s,%s) for variable of types %s, the value was %s for valid types see: %s",
                expected,
                expected.getClass().getSimpleName(),
                instance.serialise(),
                actual,
                "http://www.scassandra.org/java-client/column-types/"
        ));
    }

}
