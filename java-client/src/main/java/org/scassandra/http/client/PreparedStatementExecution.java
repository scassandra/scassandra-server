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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import org.scassandra.cql.CqlType;
import org.scassandra.server.cqlmessages.types.CqlText;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class PreparedStatementExecution {
    private final String preparedStatementText;
    private final String consistency;
    private final List<Object> variables;
    private List<CqlType> variableTypes;

    private PreparedStatementExecution(String preparedStatementText, String consistency, List<Object> variables, List<CqlType> variableTypes) {
        this.preparedStatementText = preparedStatementText;
        this.consistency = consistency;
        this.variables = variables;
        this.variableTypes = variableTypes;
    }

    public String getPreparedStatementText() {
        return preparedStatementText;
    }

    public String getConsistency() {
        return consistency;
    }

    public List<Object> getVariables() {
        return Collections.unmodifiableList(variables);
    }

    @Override
    public String toString() {
        return "PreparedStatementExecution{" +
                "preparedStatementText='" + preparedStatementText + '\'' +
                ", consistency='" + consistency + '\'' +
                ", variables=" + variables +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PreparedStatementExecution that = (PreparedStatementExecution) o;

        if (consistency != null ? !consistency.equals(that.consistency) : that.consistency != null) return false;
        if (preparedStatementText != null ? !preparedStatementText.equals(that.preparedStatementText) : that.preparedStatementText != null)
            return false;
        if (variables != null ? !variables.equals(that.variables) : that.variables != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = preparedStatementText != null ? preparedStatementText.hashCode() : 0;
        result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
        result = 31 * result + (variables != null ? variables.hashCode() : 0);
        return result;
    }

    public static PreparedStatementExecutionBuilder builder() {
        return new PreparedStatementExecutionBuilder();
    }

    /**
     * Don't use this builder for creating PreparedStatementExecutions to match against.
     *
     * @param variableTypes The types of the varialbes
     * @return A PreparedStatementExecutionBuilder
     */
    public static PreparedStatementExecutionBuilder builder(List<ColumnTypes> variableTypes) {
        return new PreparedStatementExecutionBuilder(variableTypes);
    }

    /**
     * Don't use this builder for creating PreparedStatementExecutions to match against.
     *
     * @param variableTypes The types of the variables
     * @return A PreparedStatementExecutionBuilder
     */
    public static PreparedStatementExecutionBuilder builder(ColumnTypes... variableTypes) {
        return new PreparedStatementExecutionBuilder(Arrays.asList(variableTypes));
    }

    public List<CqlType> getVariableTypes() {
        return variableTypes;
    }

    public static class PreparedStatementExecutionBuilder {

        private List<CqlType> variableTypes = Collections.emptyList();
        private String preparedStatementText;
        private String consistency = "ONE";
        private List<Object> variables = Collections.emptyList();

        private PreparedStatementExecutionBuilder() {
        }

        /**
         * @deprecated Use CqlType instead
         */
        @Deprecated
        public PreparedStatementExecutionBuilder(List<ColumnTypes> variableTypes) {
            this.variableTypes = FluentIterable.from(variableTypes).transform(new Function<ColumnTypes, CqlType>() {
                @Override
                public CqlType apply(ColumnTypes input) {
                    return input.getType();
                }
            }).toList();
        }

        public PreparedStatementExecutionBuilder(CqlType... variableTypes) {
            this.variableTypes = Arrays.asList(variableTypes);
        }

        /**
         * Defaults to ONE if not set.
         *
         * @param consistency Query consistency
         * @return this builder
         */
        public PreparedStatementExecutionBuilder withConsistency(String consistency) {
            this.consistency = consistency;
            return this;
        }

        public PreparedStatementExecutionBuilder withPreparedStatementText(String preparedStatementText) {
            this.preparedStatementText = preparedStatementText;
            return this;
        }

        public PreparedStatementExecutionBuilder withVariables(Object... variables) {
            this.variables = Arrays.asList(variables);
            return this;
        }

        public PreparedStatementExecutionBuilder withVariables(List<Object> variables) {
            this.variables = variables;
            return this;
        }

        public PreparedStatementExecution build() {
            if (preparedStatementText == null) {
                throw new IllegalStateException("Must set PreparedStatementExecutionBuilder");
            }
            return new PreparedStatementExecution(this.preparedStatementText, this.consistency, this.variables, this.variableTypes);
        }
    }
}
