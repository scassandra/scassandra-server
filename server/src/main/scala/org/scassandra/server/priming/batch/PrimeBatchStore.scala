/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
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
package org.scassandra.server.priming.batch

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.codec.Consistency
import org.scassandra.server.priming.BatchExecution
import org.scassandra.server.priming.query.Prime

import scala.collection.mutable.ListBuffer

class PrimeBatchStore extends LazyLogging {

  private val primes: ListBuffer[BatchPrimeSingle] = new ListBuffer[BatchPrimeSingle]
  
  def record(prime: BatchPrimeSingle): Unit = {
    primes += prime
  }

  def apply(primeMatch: BatchExecution): Option[Prime] = {
    primes.find { prime =>
      prime.when.queries == primeMatch.batchQueries.map(bq => BatchQueryPrime(bq.query, bq.batchQueryKind)) &&
      prime.when.consistency.getOrElse(Consistency.all).contains(primeMatch.consistency) &&
      (prime.when.batchType.isEmpty || prime.when.batchType.get == primeMatch.batchType)
    }.map(_.prime)
  }
  def clear() = {
    primes = Map()
  }
}
