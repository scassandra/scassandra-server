package org.scassandra.server.e2e.query

import com.datastax.driver.core.exceptions.{WriteTimeoutException, ReadTimeoutException, UnavailableException}
import com.datastax.driver.core.{ConsistencyLevel, SimpleStatement}
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.{Unavailable, WriteTimeout, ReadTimeout}
import org.scassandra.server.priming.query.{Then, When}

class PrimingErrorsTest extends AbstractIntegrationTest {

  test("ReadTimeout: should return the consistency passed ini") {
    prime(When(queryPattern = Some(".*")), Then(None, result = Some(ReadTimeout)))
    val statement = new SimpleStatement("select * from some_table")
    val consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM
    statement.setConsistencyLevel(consistency)

    val exception = intercept[ReadTimeoutException] {
      session.execute(statement)
    }

    exception.getConsistencyLevel should equal(consistency)
  }

  test("WriteTimeout: should return the consistency passed ini") {
    prime(When(queryPattern = Some(".*")), Then(None, result = Some(WriteTimeout)))
    val statement = new SimpleStatement("insert into something")
    val consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM
    statement.setConsistencyLevel(consistency)

    val exception = intercept[WriteTimeoutException] {
      session.execute(statement)
    }

    exception.getConsistencyLevel should equal(consistency)
  }

  test("Unavailable: should return the consistency passed ini") {
    prime(When(queryPattern = Some(".*")), Then(None, result = Some(Unavailable)))
    val statement = new SimpleStatement("insert into something")
    val consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM
    statement.setConsistencyLevel(consistency)

    val exception = intercept[UnavailableException] {
      session.execute(statement)
    }

    exception.getConsistencyLevel should equal(consistency)
  }
}
