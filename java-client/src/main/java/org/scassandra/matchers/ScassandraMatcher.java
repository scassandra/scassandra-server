package org.scassandra.matchers;

import org.hamcrest.TypeSafeMatcher;
import org.scassandra.cql.CqlType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

abstract class ScassandraMatcher<T extends List<S>, S> extends TypeSafeMatcher<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScassandraMatcher.class);

    @Override
    protected boolean matchesSafely(T queries) {
        for (int i = 0; i < queries.size(); i++) {
            S actual = queries.get(i);
            try {
                if (match(actual)) {
                    return true;
                }
            } catch (IllegalArgumentException e) {
                // if it is the last one let this out
                if (i == queries.size() - 1) {
                    throw e;
                } else {
                    LOGGER.info("Found prepared statement execution that didn't match: {}, reason: {}", actual, e.getMessage());
                }
            }
        }
        return false;
    }

    protected abstract boolean match(S match);

    protected boolean checkVariables(List<Object> expectedVariables, List<CqlType> variableTypes, List<Object> actualVariables) {
        if (variableTypes.size() != actualVariables.size()) {
            throw new IllegalArgumentException(String.format("Server has returned a different number of variables to variable types: variables %s variableTypes %s", actualVariables, variableTypes));
        }
        if (expectedVariables.size() != actualVariables.size()) {
            return false;
        }


        for (int index = 0; index < expectedVariables.size(); index++) {

            Object expectedVariable = expectedVariables.get(index);
            Object actualVariable = actualVariables.get(index);
            CqlType columnType = variableTypes.get(index);
            if (!columnType.equals(expectedVariable, actualVariable)) return false;

        }
        return true;
    }

}
