package org.scassandra.http.client;

/**
 * Not reusing BatchQuery as we'll want to put regular expressions and variables here
 * at some point
 */
public final class BatchQueryPrime {

    public static BatchQueryPrimeBuilder batchQueryPrime() {
        return new BatchQueryPrimeBuilder();
    }

    public static BatchQueryPrime batchQueryPrime(String query, BatchQueryKind kind) {
        return new BatchQueryPrime(query, kind);
    }

    private final String text;
    private final BatchQueryKind kind;

    private BatchQueryPrime(String text, BatchQueryKind kind) {
        this.text = text;
        this.kind = kind;
    }

    public String getText() {
        return text;
    }

    public BatchQueryKind getKind() {
        return kind;
    }
    @Override
    public String toString() {
        return "BatchQueryPrime{" +
                "text='" + text + '\'' +
                ", kind=" + kind +
                '}';
    }

    public static class BatchQueryPrimeBuilder {
        private String query;
        private BatchQueryKind kind;

        private BatchQueryPrimeBuilder() {
        }

        public BatchQueryPrimeBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public BatchQueryPrimeBuilder withKind(BatchQueryKind kind) {
            this.kind = kind;
            return this;
        }

        public BatchQueryPrime build() {
            return new BatchQueryPrime(query, kind);
        }
    }
}
