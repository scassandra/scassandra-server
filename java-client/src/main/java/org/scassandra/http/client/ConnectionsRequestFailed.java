package org.scassandra.http.client;

public class ConnectionsRequestFailed extends RuntimeException {

    public ConnectionsRequestFailed(String msg, Throwable cause) {
        super(msg, cause);
    }
}
