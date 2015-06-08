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
package org.scassandra.http.client;

public final class Query {
    
    public static class QueryBuilder {

        private String query;
        private String consistency = "ONE";

        private QueryBuilder() {}

        public QueryBuilder withQuery(String query){
            this.query = query;
            return this;
        }

        /**
         * Defaults to ONE if not set.
         * @param consistency Query consistency
         * @return this builder
         */
        public QueryBuilder withConsistency(String consistency){
            this.consistency = consistency;
            return this;
        }
        
        public Query build(){
            if (query == null) {
                throw new IllegalStateException("Must set query");
            }
            return new Query(this.query, this.consistency);
        }
    }

    public static QueryBuilder builder() { return new QueryBuilder(); }

    private final String query;
    private final String consistency;

    private Query(String query, String consistency) {
        this.query = query;
        this.consistency = consistency;
    }

    public String getQuery() {
        return query;
    }

    public String getConsistency() {
        return consistency;
    }

    @Override
    public String toString() {
        return "Query{" +
                "query='" + query + '\'' +
                ", consistency='" + consistency + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Query query1 = (Query) o;

        if (consistency != null ? !consistency.equals(query1.consistency) : query1.consistency != null) return false;
        if (query != null ? !query.equals(query1.query) : query1.query != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = query != null ? query.hashCode() : 0;
        result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
        return result;
    }
}
