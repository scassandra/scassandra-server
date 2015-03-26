package org.scassandra.server.priming

import org.apache.cassandra.db.WriteType

/**
 * Represents the result of the prime in the system. See ResultJsonRepresentation for
 * serialisation of prime JSON.
 */
abstract class PrimeResult

case object SuccessResult extends PrimeResult
case class ReadRequestTimeoutResult(receivedResponses: Int = 0, requiredResponses: Int = 1, dataPresent: Boolean = false) extends PrimeResult
case class WriteRequestTimeoutResult(receivedResponses: Int = 0, requiredResponses: Int = 1, writeType: WriteType = WriteType.SIMPLE) extends PrimeResult
case class UnavailableResult(requiredResponses: Int = 1, alive: Int = 0) extends PrimeResult
