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

        public BatchPrimingRequest build() {
            return new BatchPrimingRequest(new BatchWhen(consistency, queries), then);
        }
    }

    public final static class BatchWhen {
        private final List<PrimingRequest.Consistency> consistency;
        private final List<BatchQueryPrime> queries;

        private BatchWhen(List<PrimingRequest.Consistency> consistency, List<BatchQueryPrime> queries) {
            this.consistency = consistency;
            this.queries = queries;
        }

        public List<PrimingRequest.Consistency> getConsistency() {
            return Collections.unmodifiableList(consistency);
        }
        public List<BatchQueryPrime> getQueries() {
            return Collections.unmodifiableList(queries);
        }

    }

}
