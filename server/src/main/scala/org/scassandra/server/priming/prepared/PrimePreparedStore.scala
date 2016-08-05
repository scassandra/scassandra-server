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
package org.scassandra.server.priming.prepared

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.codec._
import org.scassandra.server.priming.query.Prime

class PrimePreparedStore extends PreparedStore[PrimePreparedSingle] with LazyLogging {

  def apply(queryText: String, execute: Execute) : Option[Prime] = {
    // Find prime matching queryText and execute's consistency.
    val prime = retrievePrimes().find { prime =>
      // if no consistency specified in the prime, allow all
      val c = prime.when.consistency.getOrElse(Consistency.all)
      prime.when.query.exists(_.equals(queryText)) && c.contains(execute.parameters.consistency)
    }

    prime.map(_.thenDo.prime)
  }

}
