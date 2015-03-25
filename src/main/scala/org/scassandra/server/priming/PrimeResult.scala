package org.scassandra.server.priming

/**
 * Represents the result of the prime in the system. See ResultJsonRepresentation for
 * serialisation of prime JSON.
 */
abstract class PrimeResult

case object SuccessResult extends PrimeResult
case class ReadRequestTimeoutResult(receivedResponses: Int = 0, requiredResponses: Int = 1, dataPresent: Boolean = false) extends PrimeResult
case class WriteRequestTimeoutResult() extends PrimeResult
case class UnavailableResult() extends PrimeResult
