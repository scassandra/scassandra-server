package org.scassandra.server.e2e.query

import com.datastax.driver.core.exceptions.{WriteTimeoutException, ReadTimeoutException, UnavailableException}
import com.datastax.driver.core.{WriteType, ConsistencyLevel, SimpleStatement}
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.{ErrorConstants, Unavailable, WriteTimeout, ReadTimeout}
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

  test("ReadTimeout: with required / received and data present set") {
    val properties = Map[String, String](
      ErrorConstants.ReceivedResponse -> "2",
      ErrorConstants.RequiredResponse -> "3",
      ErrorConstants.DataPresent -> "true")

    prime(When(queryPattern = Some(".*")), Then(None, result = Some(ReadTimeout), config = Some(properties)))

    val statement = new SimpleStatement("select * from some_table")
    val consistency: ConsistencyLevel = ConsistencyLevel.ALL
    statement.setConsistencyLevel(consistency)

    val exception = intercept[ReadTimeoutException] {
      session.execute(statement)
    }

    exception.getConsistencyLevel should equal(consistency)
    exception.getReceivedAcknowledgements should equal(2)
    exception.getRequiredAcknowledgements should equal(3)
    exception.wasDataRetrieved() should equal(true)
  }

  test("WriteTimeout: with required / received and data present set") {
    val properties = Map[String, String](
      ErrorConstants.ReceivedResponse -> "2",
      ErrorConstants.RequiredResponse -> "3",
      ErrorConstants.WriteType -> "BATCH")

    prime(When(queryPattern = Some(".*")), Then(None, result = Some(WriteTimeout), config = Some(properties)))

    val statement = new SimpleStatement("select * from some_table")
    val consistency: ConsistencyLevel = ConsistencyLevel.ALL
    statement.setConsistencyLevel(consistency)

    val exception = intercept[WriteTimeoutException] {
      session.execute(statement)
    }

    exception.getConsistencyLevel should equal(consistency)
    exception.getReceivedAcknowledgements should equal(2)
    exception.getRequiredAcknowledgements should equal(3)
    exception.getWriteType should equal(WriteType.BATCH)
  }
}
