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
import org.scassandra.http.client.Query;

import java.util.List;

class QueryMatcher extends TypeSafeMatcher<List<Query>> {

    private Query query;

    public  QueryMatcher(Query query) {
        if (query == null) throw new IllegalArgumentException("null query");
        this.query = query;
    }

    @Override
    protected boolean matchesSafely(List<Query> queries) {
        for (int i = 0; i < queries.size(); i++) {
            if (matchesQuery(queries.get(0))) {
                return true;
            }
        }
        return false;
    }

    private boolean matchesQuery(Query actualQuery) {

        List<CqlType> variableTypes = actualQuery.getVariableTypes();
        List<Object> actualVariables = actualQuery.getVariables();
        if (variableTypes.size() != actualVariables.size()) {
            throw new IllegalArgumentException(String.format("Server has returned a different number of variables to variable types: variables %s variableTypes %s", actualVariables, variableTypes));
        }


        if (!actualQuery.getConsistency().equals(expectedPreparedStatementExecution.getConsistency()))
            return false;
        if (!actualQuery.getPreparedStatementText().equals(expectedPreparedStatementExecution.getPreparedStatementText()))
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

    @Override
    public void describeMismatchSafely(List<Query> actual, Description description) {
        description.appendText("the following queries were executed: ");
        for (Query query : actual) {
            description.appendText("\n" + query);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Expected query " + query + " to be executed");
    }
}