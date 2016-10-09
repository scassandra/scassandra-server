package org.scassandra.cql;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Set;

public class SetType extends CqlType {

    public static SetType set(CqlType type) {
        return new SetType(type);
    }

    private final CqlType type;

    SetType(CqlType type) {
        this.type = type;
    }

    @Override
    public String serialise() {
        return String.format("set<%s>", type.serialise());
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        if (expected instanceof Set) {
            final Set<?> typedExpected = (Set<?>) expected;
            final List<?> actualList = (List<?>) actual;

            return typedExpected.size() == actualList.size() &&
                    Iterables.all(typedExpected, new Predicate<Object>() {
                        @Override
                        public boolean apply(final Object eachExpected) {
                            return Iterables.any(actualList, new Predicate<Object>() {
                                @Override
                                public boolean apply(Object eachActual) {
                                    return type.equals(eachExpected, eachActual);
                                }
                            });
                        }
                    });

        } else {
            throw throwInvalidType(expected, actual, this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SetType setType = (SetType) o;

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
