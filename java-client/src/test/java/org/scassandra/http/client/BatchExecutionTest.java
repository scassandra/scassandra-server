package org.scassandra.http.client;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class BatchExecutionTest {
    @Test
    public void testEquals() throws Exception {
        EqualsVerifier.forClass(BatchExecution.class)
                .allFieldsShouldBeUsed()
                .verify();
    }
}