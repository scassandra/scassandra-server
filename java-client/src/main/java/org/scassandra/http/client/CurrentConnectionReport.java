package org.scassandra.http.client;

import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class CurrentConnectionReport implements ConnectionReport {
    private final List<ClientConnection> connections;

    public CurrentConnectionReport(List<ClientConnection> connections) {
        this.connections = connections;
    }

    @Override
    public List<ClientConnection> getConnections() {
        return connections;
    }

    @Override
    public List<InetSocketAddress> getAddresses() {
        return Lists.transform(this.connections, new Function<ClientConnection, InetSocketAddress>() {
            @Override
            public InetSocketAddress apply(ClientConnection input) {
                return input.getAddress();
            }
        });
    }
}
