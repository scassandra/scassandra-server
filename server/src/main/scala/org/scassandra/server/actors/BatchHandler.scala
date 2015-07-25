package org.scassandra.server.actors

import akka.actor.{ActorRef, Actor}
import akka.util.ByteString
import org.scassandra.server.actors.BatchHandler.Execute
import org.scassandra.server.cqlmessages.{Consistency, CqlProtocolHelper, CqlMessageFactory}
import org.scassandra.server.priming.{BatchExecution, BatchStatement, ActivityLog}

class BatchHandler(tcpConnection: ActorRef, msgFactory: CqlMessageFactory, activityLog: ActivityLog) extends Actor {

  import CqlProtocolHelper._

  override def receive: Receive = {
    case Execute(body, stream) =>
      val iterator = body.iterator
      val batchType = iterator.getByte
      val numStatements = iterator.getShort

      val statements: List[BatchStatement] = (0 until numStatements).map(_ => {
        val kind = iterator.getByte
        assert(kind == 0)
        val query: String = readLongString(iterator).get
        val numberOfParameters = iterator.getShort
        assert(numberOfParameters == 0)
        BatchStatement(query)
      }).toList

      val consistency = Consistency.fromCode(iterator.getShort)

      activityLog.recordBatchExecution(BatchExecution(statements, consistency))

      tcpConnection ! msgFactory.createVoidMessage(stream)
  }
}

object BatchHandler {
  case class Execute(body: ByteString, stream: Byte)
}
