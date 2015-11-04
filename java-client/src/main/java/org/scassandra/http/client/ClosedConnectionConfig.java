package org.scassandra.http.client;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.scassandra.server.priming.ErrorConstants;

public class ClosedConnectionConfig extends Config {

    public enum CloseType {
        CLOSE,
        RESET,
        HALFCLOSE;
    }

    private final CloseType closeType;

    public ClosedConnectionConfig(CloseType closeType) {
        this.closeType = closeType;
    }

    @Override
    Map<String, ?> getProperties() {
        return ImmutableMap.of(
            ErrorConstants.CloseType(), String.valueOf(closeType.toString().toLowerCase())
        );
    }
}
