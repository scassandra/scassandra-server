package uk.co.scassandra.server

import com.typesafe.scalalogging.slf4j.{Logging}
import akka.actor.{ActorRef, Actor}
import akka.util.{ByteIterator, ByteString}
import uk.co.scassandra.cqlmessages.response.{CqlMessageFactory}
import uk.co.scassandra.cqlmessages.{ColumnType, CqlVarchar, CqlProtocolHelper}
import uk.co.scassandra.priming.prepared.PrimePreparedStore
import uk.co.scassandra.priming.query.PrimeMatch

class PrepareHandler(primePreparedStore: PrimePreparedStore) extends Actor with Logging {

  import CqlProtocolHelper._

  var preparedStatementId : Int = 1
  var preparedStatementsToId : Map[Int, String] = Map()

  def receive: Actor.Receive = {
    case PrepareHandlerMessages.Prepare(body, stream, msgFactory, connection) => {
      logger.debug(s"Received prepare message $body")
      val query = CqlProtocolHelper.readLongString(body.iterator)
      logger.debug(s"Prepare for query $query")
      val numberOfParameters = query.toCharArray.filter(_ == '?').size
      val columnTypes: Map[String, ColumnType] = (0 until numberOfParameters).map(num => (num.toString,CqlVarchar)).toMap
      val preparedResult = msgFactory.createPreparedResult(stream, preparedStatementId, columnTypes)

      preparedStatementsToId += (preparedStatementId -> query)

      preparedStatementId = preparedStatementId + 1

      logger.debug(s"Sending back prepared result ${preparedResult}")
      connection ! preparedResult
    }
    case PrepareHandlerMessages.Execute(body, stream, msgFactory, connection) => {
      logger.debug(s"Received execute message $body")
      val bodyIterator: ByteIterator = body.iterator
      // length of the id - this is a short
      bodyIterator.drop(2)
      val preparedStatementId = bodyIterator.getInt

      val query = preparedStatementsToId.get(preparedStatementId)

      if (query.isDefined) {
        val prime = primePreparedStore.findPrime(PrimeMatch(query.get))

        prime match {
          case Some(prime) => connection ! msgFactory.createRowsMessage(prime, stream)
          case None => connection ! msgFactory.createVoidMessage(stream)
        }
      } else {
        connection ! msgFactory.createVoidMessage(stream)
      }
    }
    case msg @ _ => {
      logger.debug(s"Received unknown message $msg")
    }
  }
}

object PrepareHandlerMessages {
  case class Prepare(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef)
  case class Execute(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef)
}
