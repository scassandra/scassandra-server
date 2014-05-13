package uk.co.scassandra.server

import com.typesafe.scalalogging.slf4j.{Logging}
import akka.actor.{ActorRef, Actor}
import akka.util.{ByteIterator, ByteString}
import uk.co.scassandra.cqlmessages.response.{CqlMessageFactory}
import uk.co.scassandra.cqlmessages._
import uk.co.scassandra.priming.prepared.PrimePreparedStore
import uk.co.scassandra.priming.query.PrimeMatch
import uk.co.scassandra.priming._
import uk.co.scassandra.priming.query.PrimeMatch
import scala.Some
import uk.co.scassandra.priming.query.PrimeMatch
import scala.Some

class PrepareHandler(primePreparedStore: PrimePreparedStore) extends Actor with Logging {

  import CqlProtocolHelper._

  var preparedStatementId : Int = 1
  var preparedStatementsToId : Map[Int, String] = Map()

  def receive: Actor.Receive = {
    case PrepareHandlerMessages.Prepare(body, stream, msgFactory, connection) => {
      logger.debug(s"Received prepare message $body")
      val query = CqlProtocolHelper.readLongString(body.iterator)
      logger.debug(s"Prepare for query $query")

      val preparedPrime = primePreparedStore.findPrime(PrimeMatch(query))
      val preparedResult = if (preparedPrime.isDefined) {
        msgFactory.createPreparedResult(stream, preparedStatementId, preparedPrime.get.variableTypes)
      } else {
        val numberOfParameters = query.toCharArray.filter(_ == '?').size
        val variableTypes: List[ColumnType] = (0 until numberOfParameters).map(num => CqlVarchar).toList
        msgFactory.createPreparedResult(stream, preparedStatementId, variableTypes)
      }

      preparedStatementsToId += (preparedStatementId -> query)

      preparedStatementId = preparedStatementId + 1

      logger.debug(s"Sending back prepared result ${preparedResult}")
      connection ! preparedResult
    }
    case PrepareHandlerMessages.Execute(body, stream, msgFactory, connection) => {
      logger.trace(s"Received execute message $body")
      val bodyIterator: ByteIterator = body.iterator
      // length of the id - this is a short
      bodyIterator.drop(2)
      val preparedStatementId = bodyIterator.getInt
      val consistency = Consistency.fromCode(bodyIterator.getShort)
      val preparedStatement = preparedStatementsToId.get(preparedStatementId)

      if (preparedStatement.isDefined) {
        ActivityLog.recordPrimedStatementExecution(preparedStatement.get, consistency, List())
        val prime = primePreparedStore.findPrime(PrimeMatch(preparedStatement.get))
        logger.debug(s"Prime for prepared statement query: $preparedStatement prime: $prime")
        prime match {
          case Some(preparedPrime) => {
           preparedPrime.prime.result match {
             case Success => connection ! msgFactory.createRowsMessage(preparedPrime.prime, stream)
             case ReadTimeout => connection ! msgFactory.createReadTimeoutMessage(stream)
             case WriteTimeout => connection ! msgFactory.createWriteTimeoutMessage(stream)
             case Unavailable => connection ! msgFactory.createUnavailableMessage(stream)
           }

          }
          case None => connection ! msgFactory.createVoidMessage(stream)
        }
      } else {
        logger.debug(s"Didn't find prepared statement. Sending back a void result.")
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
