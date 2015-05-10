package org.scassandra.server.actors

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.util.ByteString
import org.scassandra.server.cqlmessages.CqlMessageFactory
import org.scassandra.server.cqlmessages.request.ExecuteRequest
import org.scassandra.server.cqlmessages.response.Result
import org.scassandra.server.cqlmessages.types.CqlVarchar
import org.scassandra.server.priming._
import org.scassandra.server.priming.prepared.{PreparedPrime, PreparedStoreLookup}
import org.scassandra.server.priming.query.PrimeMatch

import scala.concurrent.duration.FiniteDuration

class PrepareHandler(primePreparedStore: PreparedStoreLookup, activityLog: ActivityLog) extends Actor with ActorLogging {

  import org.scassandra.server.cqlmessages.CqlProtocolHelper._

  private var preparedStatementId: Int = 1
  private var preparedStatementsToId: Map[Int, String] = Map()

  def receive: Actor.Receive = {
    case PrepareHandlerMessages.Prepare(body, stream, msgFactory: CqlMessageFactory, connection) =>
      val preparedResult: Result = handlePrepare(body, stream, msgFactory)
      connection ! preparedResult

    case PrepareHandlerMessages.Execute(body, stream, msgFactory, connection) =>
      val action: ExecuteResponse = handleExecute(body, stream, msgFactory)
      sendMessage(action.msg, connection)
      action.activity.foreach(activityLog.recordPreparedStatementExecution)
  }
  
  case class MessageWithDelay(msg: Any, delay: Option[FiniteDuration] = None)
  case class ExecuteResponse(activity: Option[PreparedStatementExecution], msg: MessageWithDelay)

  private def handleExecute(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory): ExecuteResponse = {
    val executeRequest = msgFactory.parseExecuteRequestWithoutVariables(stream, body)
    log.debug(s"Received execute message $executeRequest")

    val prepStatement = preparedStatementsToId.get(executeRequest.id)

    val action = prepStatement match {
      case Some(p) =>
        val matchingPrimedAction = for {
          prime <- primePreparedStore.findPrime(PrimeMatch(p, executeRequest.consistency))
          if executeRequest.numberOfVariables == prime.variableTypes.size
          parsed = msgFactory.parseExecuteRequestWithVariables(stream, body, prime.variableTypes)
          pse = PreparedStatementExecution(p, parsed.consistency, parsed.variables, prime.variableTypes)
        } yield ExecuteResponse(Some(pse), MessageWithDelay(createMessage(prime, executeRequest, stream, msgFactory), prime.prime.fixedDelay))

        lazy val defaultAction = ExecuteResponse(Some(PreparedStatementExecution(p, executeRequest.consistency, List(), List())),
          MessageWithDelay(msgFactory.createVoidMessage(stream)))

        matchingPrimedAction.getOrElse(defaultAction)
      case None => statementNotRecognised(stream, msgFactory)
    }
    action
  }

  private def handlePrepare(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory): Result = {
    val query: String = readLongString(body.iterator).get
    val preparedPrime = primePreparedStore.findPrime(PrimeMatch(query))

    val preparedResult = preparedPrime
      .map(prime => msgFactory.createPreparedResult(stream, preparedStatementId, prime.variableTypes))
      .getOrElse({
      val numberOfParameters = query.toCharArray.count(_ == '?')
      val variableTypes = (0 until numberOfParameters).map(num => CqlVarchar).toList
      msgFactory.createPreparedResult(stream, preparedStatementId, variableTypes)
    })
    preparedStatementsToId += (preparedStatementId -> query)
    preparedStatementId = preparedStatementId + 1
    log.info(s"Prepared Statement has been prepared: |$query|. Prepared result is: $preparedResult")

    preparedResult
  }

  private def statementNotRecognised(stream: Byte, msgFactory: CqlMessageFactory): ExecuteResponse = {
    ExecuteResponse(None, MessageWithDelay(msgFactory.createVoidMessage(stream)))
  }

  private def createMessage(preparedPrime: PreparedPrime, executeRequest: ExecuteRequest ,stream: Byte, msgFactory: CqlMessageFactory) = {
    preparedPrime.prime.result match {
      case SuccessResult => msgFactory.createRowsMessage(preparedPrime.prime, stream)
      case result: ReadRequestTimeoutResult => msgFactory.createReadTimeoutMessage(stream, executeRequest.consistency, result)
      case result: WriteRequestTimeoutResult => msgFactory.createWriteTimeoutMessage(stream, executeRequest.consistency, result)
      case result: UnavailableResult => msgFactory.createUnavailableMessage(stream, executeRequest.consistency, result)
    }
  }

  private def sendMessage(msgAndDelay: MessageWithDelay, receiver: ActorRef) = {
    msgAndDelay.delay match {
      case None => receiver ! msgAndDelay.msg
      case Some(duration) =>
        log.info(s"Delaying response of prepared statement by $duration")
        context.system.scheduler.scheduleOnce(duration, receiver, msgAndDelay.msg)(context.system.dispatcher)
    }
  }
}

object PrepareHandlerMessages {
  case class Prepare(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef)
  case class Execute(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef)
}