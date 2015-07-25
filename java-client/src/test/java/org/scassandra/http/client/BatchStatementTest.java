package org.scassandra.http.client;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class BatchStatementTest {
    @Test
    public void testEquals() throws Exception {
        EqualsVerifier.forClass(BatchStatement.class)
                .allFieldsShouldBeUsed()
                .verify();
    }
}