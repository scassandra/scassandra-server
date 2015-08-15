package org.scassandra.http.client;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.scassandra.server.priming.ErrorConstants;

public class ErrorMessageConfig extends Config {

    private final String message;

    public ErrorMessageConfig(String message) {
        this.message = message;
    }

    @Override
    Map<String, ?> getProperties() {
        return ImmutableMap.of(ErrorConstants.Message(), message);
    }
}
