package org.scassandra.http.client;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.scassandra.server.priming.ErrorConstants;

public class AlreadyExistsConfig extends Config {

    private String keyspace;
    private String table;
    private String message;

    public AlreadyExistsConfig(String keyspace) {
        this(keyspace, "");
    }

    public AlreadyExistsConfig(String keyspace, String table) {
        this(keyspace, table, null);
    }

    public AlreadyExistsConfig(String keyspace, String table, String message) {
        this.keyspace = keyspace;
        this.table = table;
        this.message = message;
    }

    @Override
    Map<String, ?> getProperties() {
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
        mapBuilder.put(ErrorConstants.Keyspace(), keyspace);
        mapBuilder.put(ErrorConstants.Table(), table);
        if(message != null) {
            mapBuilder.put(ErrorConstants.Message(), message);
        }
        return mapBuilder.build();
    }
}
