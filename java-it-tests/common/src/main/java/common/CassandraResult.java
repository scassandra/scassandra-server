package common;

import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.WriteTimeoutConfig;
import org.scassandra.http.client.WriteTypePrime;

import java.util.List;

public interface CassandraResult {
    List<CassandraRow> rows();

    ResponseStatus status();

    public abstract static class ResponseStatus {

        private final PrimingRequest.Result result;

        public ResponseStatus(PrimingRequest.Result result) {
            this.result = result;
        }

        public PrimingRequest.Result getResult() {
            return result;
        }
    }

    public abstract static class ErrorStatus extends ResponseStatus {
        private final String consistency;
        public ErrorStatus(PrimingRequest.Result result, String consistency) {
            super(result);
            this.consistency = consistency;
        }

        public String getConsistency() {
            return consistency;
        }

    }

    public static class SuccessStatus extends ResponseStatus {
        public SuccessStatus() {
            super(PrimingRequest.Result.success);
        }
    }

    public static class ErrorMessageStatus extends ErrorStatus {
        private final String message;

        public ErrorMessageStatus(PrimingRequest.Result result, String message) {
            super(result, null);
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    public static class ReadTimeoutStatus extends ErrorStatus {
        private final int receivedAcknowledgements;
        private final int requiredAcknowledgements;
        private final boolean wasDataRetrieved;

        public ReadTimeoutStatus(String consistency, int receivedAcknowledgements, int requiredAcknowledgements, boolean wasDataRetrieved) {
            super(PrimingRequest.Result.read_request_timeout, consistency);
            this.receivedAcknowledgements = receivedAcknowledgements;
            this.requiredAcknowledgements = requiredAcknowledgements;
            this.wasDataRetrieved = wasDataRetrieved;
        }

        public int getReceivedAcknowledgements() {
            return receivedAcknowledgements;
        }

        public int getRequiredAcknowledgements() {
            return requiredAcknowledgements;
        }

        public boolean WasDataRetrieved() {
            return wasDataRetrieved;
        }
    }

    public static class WriteTimeoutStatus extends ErrorStatus {
        private final int receivedAcknowledgements;
        private final int requiredAcknowledgements;
        private final WriteTypePrime writeTypePrime;

        public WriteTimeoutStatus(String consistency, int receivedAcknowledgements, int requiredAcknowledgements, WriteTypePrime writeTypePrime) {
            super(PrimingRequest.Result.write_request_timeout, consistency);
            this.receivedAcknowledgements = receivedAcknowledgements;
            this.requiredAcknowledgements = requiredAcknowledgements;
            this.writeTypePrime = writeTypePrime;
        }

        public int getReceivedAcknowledgements() {
            return receivedAcknowledgements;
        }

        public int getRequiredAcknowledgements() {
            return requiredAcknowledgements;
        }

        public WriteTypePrime getWriteTypePrime() {
            return writeTypePrime;
        }
    }

    public static class UnavailableStatus extends ErrorStatus {
        private final int requiredAcknowledgements;
        private final int alive;

        public UnavailableStatus(String consistency, int requiredAcknowledgements, int alive) {
            super(PrimingRequest.Result.unavailable, consistency);
            this.requiredAcknowledgements = requiredAcknowledgements;
            this.alive = alive;
        }

        public int getRequiredAcknowledgements() {
            return requiredAcknowledgements;
        }

        public int getAlive() {
            return alive;
        }

    }
}
