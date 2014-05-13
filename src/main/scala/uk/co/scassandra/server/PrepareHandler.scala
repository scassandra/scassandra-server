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
import java.math.BigDecimal
import java.util.UUID
import java.net.InetAddress

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
        val variableTypes: List[ColumnType[_]] = (0 until numberOfParameters).map(num => CqlVarchar).toList
        msgFactory.createPreparedResult(stream, preparedStatementId, variableTypes)
      }

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
      val consistency = Consistency.fromCode(bodyIterator.getShort)

      val flags = bodyIterator.getByte
      val numberOfVariables = bodyIterator.getShort



      val preparedStatement = preparedStatementsToId.get(preparedStatementId)

      if (preparedStatement.isDefined) {        
        val prime = primePreparedStore.findPrime(PrimeMatch(preparedStatement.get))
        logger.debug(s"Prime for prepared statement query: $preparedStatement prime: $prime")
        prime match {
          case Some(preparedPrime) => {

            if (numberOfVariables == preparedPrime.variableTypes.size) {
              val variableValues = preparedPrime.variableTypes.map (varType => varType.readValue(bodyIterator).toString )
              ActivityLog.recordPrimedStatementExecution(preparedStatement.get, consistency, variableValues)
           } else {
             ActivityLog.recordPrimedStatementExecution(preparedStatement.get, consistency, List())
             logger.warn(s"Execution of prepared statement has a different number of variables to the prime. Variables won't be recorded. $prime")
           }
            
           preparedPrime.prime.result match {
             case Success => connection ! msgFactory.createRowsMessage(preparedPrime.prime, stream)
             case ReadTimeout => connection ! msgFactory.createReadTimeoutMessage(stream)
             case WriteTimeout => connection ! msgFactory.createWriteTimeoutMessage(stream)
             case Unavailable => connection ! msgFactory.createUnavailableMessage(stream)
           }

          }
          case None => {
            logger.info("Received execution of prepared statemet that hasn't been primed so can't record variables.")
            ActivityLog.recordPrimedStatementExecution(preparedStatement.get, consistency, List())
            connection ! msgFactory.createVoidMessage(stream)
          }
        }
      } else {
        logger.warn(s"Didn't find prepared statement. Has scassandra been restarted since your application prepared the statement?")
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

/*
Example execute message body
ByteString(0, 4, // length of the prepared statement id
0, 0, 0, 1, // prepared statement id
0, 1, // consistency
5, // flags
0, 7, // number of variables
0, 0, 0, 8,    0, 0, 0, 0, 0, 0, 4, -46, -    val bigInt : java.lang.Long = 1234
0, 0, 0, 8,    0, 0, 0, 0, 0, 0, 9, 41,  -    val counter : java.lang.Long = 2345
0, 0, 0, 5,    0, 0, 0, 0, 1,            -    val decimal : java.math.BigDecimal = new java.math.BigDecimal("1")
0, 0, 0, 8,    63, -8, 0, 0, 0, 0, 0, 0, -    val double : java.lang.Double = 1.5
0, 0, 0, 4,    64, 32, 0, 0,             -    val float : java.lang.Float = 2.5f
0, 0, 0, 4,    0, 0, 13, -128,           -    val int : java.lang.Integer = 3456
0, 0, 0, 1,   123,                       -    val varint : java.math.BigInteger = new java.math.BigInteger("123")

0, 0, 19, -120) // serial consistency?? not sure


ByteString(0, 4,
 0, 0, 0, 1,
 0, 1,
 5,
 0, 9,

 0, 0, 0, 5,    97, 115, 99, 105, 105,
 0, 0, 0, 0,
 0, 0, 0, 1,    1,
 0, 0, 0, 8,    0, 0, 1, 69, -11, 123, 55, 49,
 0, 0, 0, 16,  -84, 87, -28, 43, 59, -11, 72, -102, -128, 95, 83, -11, -80, -117, 97, -41,
 0, 0, 0, 7,   118, 97, 114, 99, 104, 97, 114,
 0, 0, 0, 16,  48, -99, -19, 96, -38, -105, 17, -29, -71, 0, -23, -112, 98, 25, 105, 100,
 0, 0, 0, 4,   127, 0, 0, 1,

 -1, -1, -1, -1, 0, 0, 19, -120)
 */
