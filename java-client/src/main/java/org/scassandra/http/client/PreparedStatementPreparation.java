package org.scassandra.http.client;

public final class PreparedStatementPreparation {

    private final String preparedStatementText;

    private PreparedStatementPreparation(String preparedStatementText) {
        this.preparedStatementText = preparedStatementText;
    }

    public String getPreparedStatementText() { return preparedStatementText; }

    @Override
    public String toString() {
        return "PreparedStatementPreparation{" +
                "preparedStatementText='" + preparedStatementText + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PreparedStatementPreparation that = (PreparedStatementPreparation) o;

        return !(preparedStatementText != null ? !preparedStatementText.equals(that.preparedStatementText) : that.preparedStatementText != null);
    }

    @Override
    public int hashCode() {
        return preparedStatementText != null ? preparedStatementText.hashCode() : 0;
    }

    public static PreparedStatementPreparationBuilder builder() {
        return new PreparedStatementPreparationBuilder();
    }

    public static class PreparedStatementPreparationBuilder {

        private String preparedStatementText;

        private PreparedStatementPreparationBuilder() {
        }

        public PreparedStatementPreparationBuilder withPreparedStatementText(String preparedStatementText) {
            this.preparedStatementText = preparedStatementText;
            return this;
        }

        public PreparedStatementPreparation build() {
            if (preparedStatementText == null) {
                throw new IllegalStateException("Must set preparedStatementText in PreparedStatementPreparationBuilder");
            }
            return new PreparedStatementPreparation(this.preparedStatementText);
        }
    }
}
