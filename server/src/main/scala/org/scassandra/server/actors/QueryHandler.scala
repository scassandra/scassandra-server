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
package org.scassandra.server.actors

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.{Frame, NoRows, ProtocolVersion, Query}
import org.scassandra.server.actors.ActivityLogActor.RecordQuery
import org.scassandra.server.actors.ProtocolActor._
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.{MatchPrime, MatchResult, Reply}

import scala.concurrent.duration._

class QueryHandler(primeQueryStore: ActorRef, activityLog: ActorRef) extends ProtocolActor {

  import context.dispatcher

  val noRows = Some(Reply(NoRows))

  def receive: Receive = {
    case ProtocolMessage(Frame(header, query: Query)) =>
      implicit val protocolVersion: ProtocolVersion = header.version.version
      implicit val timeout: Timeout = Timeout(250 milliseconds)

      log.info(s"Incoming query: $query")
      val toReply = sender()

      (primeQueryStore ? MatchPrime(query))
        .mapTo[MatchResult].map { mr =>

        val typesAndValues: Option[(List[DataType], List[Any])] = (for {
          prime <- mr.prime
          vt <- prime.variableTypes
        } yield Some(vt).zip(extractQueryVariables(query.query, query.parameters.values.map(_.map(_.value)), vt)).headOption).flatten

        typesAndValues match {
          case Some((vts, values)) =>
            activityLog ! RecordQuery(Activity.Query(query.query, query.parameters.consistency, query.parameters.serialConsistency, values,
              vts, query.parameters.timestamp))
          case None =>
            activityLog ! RecordQuery(Activity.Query(query.query, query.parameters.consistency, query.parameters.serialConsistency,
              timestamp = query.parameters.timestamp))
        }
        writePrime(query, mr.prime, header, alternative = noRows, consistency = Some(query.parameters.consistency), target = toReply)(context.system)
      }
  }


  //      val prime = primeQueryStore(query)

  // Attempt to extract values if variable types are present in the prime.
  //      val typesAndValues: Option[(List[DataType], List[Any])] = prime
  //        .flatmap(_.variabletypes)
  //        .flatmap { variabletypes =>
  //          Some(variabletypes).zip(extractqueryvariables(query.query, query.parameters.values.map(_.map(_.value)), variabletypes)).headoption
  //        }
  //      typesandvalues match {
  //        case some((variabletypes, values)) =>
  //          activitylog ! recordquery(activity.query(query.query, query.parameters.consistency, query.parameters.serialconsistency, values,
  //            variabletypes, query.parameters.timestamp))
  //        case none =>
  //          activitylog ! recordquery(activity.query(query.query, query.parameters.consistency, query.parameters.serialconsistency,
  //            timestamp = query.parameters.timestamp))
  //      }
  //      writeprime(query, prime, header, alternative = norows, consistency = some(query.parameters.consistency))
}
