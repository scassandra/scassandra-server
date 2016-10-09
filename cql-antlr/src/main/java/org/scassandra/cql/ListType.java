package org.scassandra.cql;

import java.util.List;

public class ListType extends CqlType {
    private final CqlType type;

    public static ListType list(CqlType type) {
        return new ListType(type);
    }

    ListType(CqlType type) {
        this.type = type;
    }

    @Override
    public String serialise() {
        return String.format("list<%s>", type.serialise());
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        if (expected instanceof List) {
            final List<?> typedExpected = (List<?>) expected;
            final List<?> actualList = (List<?>) actual;

            if (typedExpected.size() != actualList.size()) return false;

            for (int i = 0; i < actualList.size(); i++) {
                if (!type.equals(typedExpected.get(i), actualList.get(i))) return false;
            }
            return true;
        } else {
            throw throwInvalidType(expected, actual, this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ListType setType = (ListType) o;

        if (type != null ? !type.equals(setType.type) : setType.type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return type != null ? type.hashCode() : 0;
    }

    @Override
    public String toString() {
        return this.serialise();
    }

    public CqlType getType() {
        return type;
    }
}
