package org.scassandra.http.client;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class BatchQueryTest {
    @Test
    public void testEquals() throws Exception {
        EqualsVerifier.forClass(BatchQuery.class)
                .allFieldsShouldBeUsed()
                .verify();
    }
}