package org.scassandra.http.client;

import java.net.InetSocketAddress;
import java.util.List;

public interface ConnectionReport {
    List<ClientConnection> getConnections();

    List<InetSocketAddress> getAddresses();
}
