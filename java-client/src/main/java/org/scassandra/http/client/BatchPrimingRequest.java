package org.scassandra.http.client;

import java.util.*;
import java.util.List;

/**
 * This uses inner classes from PrimingRequest for backward compatibility purposes.
 */

public final class BatchPrimingRequest {

    private final BatchWhen when;
    private final PrimingRequest.Then then;

    private BatchPrimingRequest(BatchWhen when, PrimingRequest.Then then) {
        this.when = when;
        this.then = then;
    }

    public static BatchPrimingRequestBuilder batchPrimingRequest() {
        return new BatchPrimingRequestBuilder();
    }

    public static PrimingRequest.Then.ThenBuilder then() {
        return new PrimingRequest.Then.ThenBuilder();
    }

    public static class BatchPrimingRequestBuilder {
        private PrimingRequest.Then then;
        private List<BatchQueryPrime> queries;
        private List<PrimingRequest.Consistency> consistency;
        private BatchType type = BatchType.LOGGED;

        private BatchPrimingRequestBuilder() {
        }

        public BatchPrimingRequestBuilder withQueries(BatchQueryPrime... queries) {
            this.queries = Arrays.asList(queries);
            return this;
        }

        public BatchPrimingRequestBuilder withConsistency(PrimingRequest.Consistency... consistencies) {
            this.consistency = Arrays.asList(consistencies);
            return this;
        }

        public BatchPrimingRequestBuilder withThen(PrimingRequest.Then then) {
            this.then = then;
            return this;
        }

        public BatchPrimingRequestBuilder withThen(PrimingRequest.Then.ThenBuilder then) {
            this.then = then.build();
            return this;
        }

        public BatchPrimingRequestBuilder withType(BatchType type) {
            this.type = type;
            return this;
        }

        public BatchPrimingRequest build() {
            if (then == null) {
                throw new IllegalStateException("Must provide withThen before building");
            }
            return new BatchPrimingRequest(new BatchWhen(consistency, queries, type), then);
        }
    }

    public final static class BatchWhen {
        private final List<PrimingRequest.Consistency> consistency;
        private final List<BatchQueryPrime> queries;
        private final BatchType batchType;

        private BatchWhen(List<PrimingRequest.Consistency> consistency, List<BatchQueryPrime> queries, BatchType batchType) {
            this.consistency = consistency;
            this.queries = queries;
            this.batchType = batchType;
        }

        public List<PrimingRequest.Consistency> getConsistency() {
            return Collections.unmodifiableList(consistency);
        }

        public List<BatchQueryPrime> getQueries() {
            return Collections.unmodifiableList(queries);
        }

        public BatchType getBatchType() {
            return batchType;
        }
    }

}
