package org.scassandra.http.client;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class ClientConnectionTest {

    @Test
    public void testEqualsContract() {
        EqualsVerifier.forClass(ClientConnection.class).verify();
    }
}
