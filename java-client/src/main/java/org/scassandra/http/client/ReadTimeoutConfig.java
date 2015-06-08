/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.http.client;

import com.google.common.collect.ImmutableMap;
import org.scassandra.server.priming.ErrorConstants;

import java.util.Map;

public class ReadTimeoutConfig extends Config {
    private final int receivedAcknowledgements;
    private final int requiredAcknowledgements;
    private final boolean dataRetrieved;

    public ReadTimeoutConfig(int receivedAcknowledgements, int requiredAcknowledgements, boolean dataRetrieved) {
        this.receivedAcknowledgements = receivedAcknowledgements;
        this.requiredAcknowledgements = requiredAcknowledgements;
        this.dataRetrieved = dataRetrieved;
    }

    @Override
    Map<String, ?> getProperties() {
        return ImmutableMap.of(
                ErrorConstants.ReceivedResponse(), String.valueOf(this.receivedAcknowledgements),
                ErrorConstants.RequiredResponse(), String.valueOf(this.requiredAcknowledgements),
                ErrorConstants.DataPresent(), String.valueOf(this.dataRetrieved)
        );
    }
}
