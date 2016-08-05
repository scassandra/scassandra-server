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
import org.scassandra.codec.datatype.DataType
import org.scassandra.server.priming.Defaulter
import org.scassandra.server.priming.query.Prime

class PrimePreparedMultiStore extends PreparedStore[PrimePreparedMulti] with LazyLogging {

  def apply(queryText: String, execute: Execute): Option[Prime] = {
    // Find prime matching queryText and execute's consistency.
    val prime = retrievePrimes().find { prime =>
      // if no consistency specified in the prime, allow all
      val c = prime.when.consistency.getOrElse(Consistency.all)
      prime.when.query.exists(_.equals(queryText)) && c.contains(execute.parameters.consistency)
    }

    // Find the outcome action matching the execute parameters.
    val action = prime.flatMap { p =>
      // decode query parameters using the variable types of the prime.
      val numberOfParameters = queryText.toCharArray.count(_ == '?')
      val dataTypes = Defaulter.defaultVariableTypesToVarChar(numberOfParameters, p.thenDo.variable_types.getOrElse(Nil))
      val queryValues = execute.parameters.values.getOrElse(Nil)
      // TODO: handle named and unset values
      val values: List[Option[Any]] = dataTypes.zip(queryValues).map { case (dataType: DataType, queryValue: QueryValue) =>
        queryValue.value match {
          case Null => Some(null)
          case Unset => Some(null)
          case Bytes(bytes) => dataType.codec.decode(bytes.toBitVector).toOption.map(_.value)
        }
      }

      // Try to find an outcome whose criteria maps to the query parameters.
      p.thenDo.outcomes.find { outcome =>
        val matchers = outcome.criteria.variable_matcher
        // ensure lists are of the same length.
        if (values.size == matchers.size) {
          // Iterate through each value, decode it and check that it matches against the value.
          values.zip(matchers).forall {
            case (value, matcher) =>
              matcher.test(value)
          }
        } else {
          false
        }
      }.map(_.action)
    }

    action.map(_.prime)
  }

}
