package org.scassandra.server.e2e.verification

import com.datastax.driver.core.{SimpleStatement, BatchStatement}
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.PrimingHelper._

/**
 * E2E tests mainly in java-it-tests
 */
class BatchVerificationTest extends AbstractIntegrationTest {

  test("Test clearing of query results") {
    val batch = new BatchStatement()
    batch.add(new SimpleStatement("select * from blah"))
    session.execute(batch)

    clearRecordedBatchExecutions()

    getRecordedBatchExecutions() shouldEqual List()
  }

}
