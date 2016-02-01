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
package org.scassandra.server.actors

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.util.ByteString
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementResponse, PreparedStatementQuery}
import org.scassandra.server.cqlmessages.CqlMessageFactory
import org.scassandra.server.cqlmessages.response.Result
import org.scassandra.server.cqlmessages.types.{ColumnType, CqlVarchar}
import org.scassandra.server.priming._
import org.scassandra.server.priming.prepared.{PreparedPrime, PreparedStoreLookup}
import org.scassandra.server.priming.query.PrimeMatch


class PrepareHandler(primePreparedStore: PreparedStoreLookup, activityLog: ActivityLog) extends Actor with ActorLogging {

  import org.scassandra.server.cqlmessages.CqlProtocolHelper._

  private var nextId: Int = 1
  private var idToStatement: Map[Int, String] = Map()

  def receive: Actor.Receive = {
    case PrepareHandler.Prepare(body, stream, msgFactory: CqlMessageFactory, connection) =>
      val preparedResult: PrepareResponse = handlePrepare(body, stream, msgFactory)
      activityLog.recordPreparedStatementPreparation(preparedResult.activity)
      connection ! preparedResult.result

    case PreparedStatementQuery(ids) =>
      sender() ! PreparedStatementResponse(ids.flatMap(id => idToStatement.get(id) match {
        case Some(text) => Seq(id -> text)
        case None => Seq()
      }) toMap)
  }

  case class PrepareResponse(activity: PreparedStatementPreparation, result: Result)

  private def handlePrepare(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory): PrepareResponse = {
    val query: String = readLongString(body.iterator).get
    val preparedPrime: Option[PreparedPrime] = primePreparedStore.findPrime(PrimeMatch(query))

    val preparedResult: Result = preparedPrime
      .map(prime => msgFactory.createPreparedResult(stream, nextId, prime.variableTypes, prime.prime.columnTypes))
      .getOrElse({
      val numberOfParameters = query.toCharArray.count(_ == '?')
      val variableTypes = (0 until numberOfParameters).map(num => CqlVarchar).toList
        val columns = Map[String, ColumnType[_]]()
      msgFactory.createPreparedResult(stream, nextId, variableTypes, columns)
    })
    idToStatement += (nextId -> query)
    nextId = nextId + 1
    log.info(s"Prepared Statement has been prepared: |$query|. Prepared result is: $preparedResult")

    PrepareResponse(PreparedStatementPreparation(query), preparedResult)
  }
}

object PrepareHandler {
  case class Prepare(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef)
  case class PreparedStatementQuery(id: List[Int])
  case class PreparedStatementResponse(preparedStatementText: Map[Int, String])
}
