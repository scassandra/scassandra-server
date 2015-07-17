/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.server.priming

object WriteType extends Enumeration {
  val SIMPLE, BATCH, UNLOGGED_BATCH, COUNTER, BATCH_LOG, CAS = Value
}

/**
 * Represents the result of the prime in the system. See ResultJsonRepresentation for
 * serialisation of prime JSON.
 */
abstract class PrimeResult

case object SuccessResult extends PrimeResult
case class ReadRequestTimeoutResult(receivedResponses: Int = 0, requiredResponses: Int = 1, dataPresent: Boolean = false) extends PrimeResult
case class WriteRequestTimeoutResult(receivedResponses: Int = 0, requiredResponses: Int = 1, writeType: WriteType.Value = WriteType.SIMPLE) extends PrimeResult
case class UnavailableResult(requiredResponses: Int = 1, alive: Int = 0) extends PrimeResult