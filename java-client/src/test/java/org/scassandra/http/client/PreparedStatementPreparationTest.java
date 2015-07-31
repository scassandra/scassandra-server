package org.scassandra.http.client;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class PreparedStatementPreparationTest {

    @Test
    public void testEqualsContract() {
        EqualsVerifier.forClass(PreparedStatementPreparation.class).verify();
    }
}
