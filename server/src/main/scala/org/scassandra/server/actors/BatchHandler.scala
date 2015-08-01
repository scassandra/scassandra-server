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

import akka.actor.{ActorLogging, ActorRef, Actor}
import akka.util.ByteString
import org.scassandra.server.actors.BatchHandler.Execute
import org.scassandra.server.cqlmessages.{BatchType, Consistency, CqlProtocolHelper, CqlMessageFactory}
import org.scassandra.server.priming.{BatchExecution, BatchQuery, ActivityLog}

class BatchHandler(tcpConnection: ActorRef, msgFactory: CqlMessageFactory, activityLog: ActivityLog) extends Actor with ActorLogging {

  import CqlProtocolHelper._

  override def receive: Receive = {
    case Execute(body, stream) =>
      val iterator = body.iterator
      val batchType = BatchType.fromCode(iterator.getByte)
      val numStatements = iterator.getShort

      val statements: List[BatchQuery] = (0 until numStatements).map(_ => {
        val kind = iterator.getByte
        assert(kind == 0) // query rather than prepared statement
        val query: String = readLongString(iterator).get
        val numVariables = iterator.getShort
        // read off the bytes for each variable, we can't parse them until priming of batches is supported
        (0 until numVariables).foreach { _ =>
          val throwAway = readLongBytes(iterator)
          log.debug("throwing away bytes from batch statement variable as priming not supported yet {}", throwAway)
        }
        BatchQuery(query)
      }).toList

      val consistency = Consistency.fromCode(iterator.getShort)

      activityLog.recordBatchExecution(BatchExecution(statements, consistency, batchType))

      tcpConnection ! msgFactory.createVoidMessage(stream)
  }
}

object BatchHandler {
  case class Execute(body: ByteString, stream: Byte)
}
