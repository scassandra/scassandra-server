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

import com.google.common.collect.Lists;
import org.junit.Test;
import org.scassandra.http.client.Query;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.scassandra.cql.PrimitiveType.*;

public class QueryMatcherTest {

    @Test
    public void matchesOnQuery() throws Exception {
        Query queryToMatchAgainst = Query.builder()
                .withQuery("the query")
                .build();

        Query queryWithSameText = Query.builder()
                .withQuery("the query")
                .build();

        QueryMatcher underTest = new QueryMatcher(queryToMatchAgainst);

        boolean matched = underTest.matchesSafely(Arrays.asList(queryWithSameText));

        assertTrue(matched);
    }

    @Test
    public void matchingStringVariables() throws Exception {
        //given
        Query actualExecution = Query.builder(TEXT, ASCII, VARCHAR)
                .withQuery("same query")
                .withVariables("one", "two", "three")
                .build();
        Query expectedExecution = Query.builder()
                .withQuery("same query")
                .withVariables("one", "two", "three")
                .build();
        QueryMatcher underTest = new QueryMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }
}