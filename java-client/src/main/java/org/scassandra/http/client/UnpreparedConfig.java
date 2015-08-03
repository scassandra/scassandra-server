package org.scassandra.http.client;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.scassandra.server.priming.ErrorConstants;

public class UnpreparedConfig extends Config {

    private String prepareId;

    private String message;

    public UnpreparedConfig(String prepareId) {
        this(prepareId, null);
    }

    public UnpreparedConfig(String prepareId, String message) {
        this.prepareId = prepareId;
        this.message = message;
    }

    @Override
    Map<String, ?> getProperties() {
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
        mapBuilder.put(ErrorConstants.PrepareId(), prepareId);
        if(message != null) {
            mapBuilder.put(ErrorConstants.Message(), message);
        }
        return mapBuilder.build();
    }
}
