/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.matchers;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.scassandra.cql.CqlType;
import org.scassandra.http.client.PreparedStatementExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PreparedStatementMatcher extends TypeSafeMatcher<List<PreparedStatementExecution>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatementMatcher.class);

    private PreparedStatementExecution expectedPreparedStatementExecution;

    public PreparedStatementMatcher(PreparedStatementExecution expectedPreparedStatementExecution) {
        if (expectedPreparedStatementExecution == null)
            throw new IllegalArgumentException("null expectedPreparedStatementExecution");
        this.expectedPreparedStatementExecution = expectedPreparedStatementExecution;
    }

    @Override
    public void describeMismatchSafely(List<PreparedStatementExecution> preparedStatementExecutions, Description description) {
        description.appendText("the following prepared statements were executed: ");
        for (PreparedStatementExecution preparedStatement : preparedStatementExecutions) {
            description.appendText("\n" + preparedStatement);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Expected prepared statement " + expectedPreparedStatementExecution + " to be executed");
    }

    @Override
    protected boolean matchesSafely(List<PreparedStatementExecution> queries) {
        for (int i = 0; i < queries.size(); i++) {
            PreparedStatementExecution actualPreparedStatementExecution = queries.get(i);
            try {
                if (doesPreparedStatementMatch(actualPreparedStatementExecution)) {
                    return true;
                }
            } catch (IllegalArgumentException e) {
                // if it is the last one let this out
                if (i == queries.size() - 1) {
                    throw e;
                } else {
                    LOGGER.info("Found prepared statement execution that didn't match: {}, reason: {}", actualPreparedStatementExecution, e.getMessage());
                }
            }
        }
        return false;
    }

    /*
    The server sends back all floats and doubles as strings to preserve accuracy so we convert the
    actual variable to the expected variables type
     */
    private boolean doesPreparedStatementMatch(PreparedStatementExecution actualPreparedStatementExecution) {

        List<CqlType> variableTypes = actualPreparedStatementExecution.getVariableTypes();
        List<Object> actualVariables = actualPreparedStatementExecution.getVariables();
        if (variableTypes.size() != actualVariables.size()) {
            throw new IllegalArgumentException(String.format("Server has returned a different number of variables to variable types: variables %s variableTypes %s", actualVariables, variableTypes));
        }


        if (!actualPreparedStatementExecution.getConsistency().equals(expectedPreparedStatementExecution.getConsistency()))
            return false;
        if (!actualPreparedStatementExecution.getPreparedStatementText().equals(expectedPreparedStatementExecution.getPreparedStatementText()))
            return false;
        List<Object> expectedVariables = expectedPreparedStatementExecution.getVariables();

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
