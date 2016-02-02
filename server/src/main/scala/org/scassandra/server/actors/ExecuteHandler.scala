package org.scassandra.server.actors


import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.{ask, pipe}
import akka.util.{ByteString, Timeout}
import org.scassandra.server.actors.ExecuteHandler.DeserializedExecute
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.cqlmessages.{CqlProtocolHelper, CqlMessageFactory}
import org.scassandra.server.cqlmessages.CqlProtocolHelper.{bytes2Hex, serializeInt, serializeShortBytes}
import org.scassandra.server.cqlmessages.request.ExecuteRequest
import org.scassandra.server.priming._
import org.scassandra.server.priming.prepared.{PreparedPrime, PreparedStoreLookup}
import org.scassandra.server.priming.query.PrimeMatch

import scala.concurrent.duration._
import scala.language.postfixOps


class ExecuteHandler(primePreparedStore: PreparedStoreLookup, activityLog: ActivityLog, prepareHandler: ActorRef) extends Actor with ActorLogging {

  import context.dispatcher
  implicit val timeout: Timeout = 1 second

  def receive: Receive = {
    case ExecuteHandler.Execute(body, stream, msgFactory, connection) =>
      val executeRequest = msgFactory.parseExecuteRequestWithoutVariables(stream, body)
      val prepStatement = (prepareHandler ? PreparedStatementQuery(List(executeRequest.id)))
        .mapTo[PreparedStatementResponse]
        .map(res => DeserializedExecute(executeRequest, res.preparedStatementText.get(executeRequest.id), body, stream, msgFactory, connection))
      prepStatement.pipeTo(self)

    case DeserializedExecute(request, text, body, stream, msgFactory, connection) =>
      val action = handleExecute(body, stream, msgFactory, text, request, connection)
      action.activity.foreach(activityLog.recordPreparedStatementExecution)
      sendMessage(action.msg, connection)
  }

  private def handleExecute(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, prepStatement: Option[String], executeRequest: ExecuteRequest, connection: ActorRef): ExecuteResponse = {
    val action = prepStatement match {
      case Some(p) =>
        val matchingPrimedAction = for {
          prime <- primePreparedStore.findPrime(PrimeMatch(p, executeRequest.consistency))
          if executeRequest.numberOfVariables == prime.variableTypes.size || executeRequest.numberOfVariables == prime.prime.columnTypes.size
          columnTypes = if (executeRequest.numberOfVariables == prime.variableTypes.size) prime.variableTypes else prime.prime.columnTypes.values.toList
          parsed = msgFactory.parseExecuteRequestWithVariables(stream, body, columnTypes)
          pse = PreparedStatementExecution(p, parsed.consistency, parsed.variables, columnTypes)
        } yield ExecuteResponse(Some(pse), MessageWithDelay(createMessage(prime, executeRequest, stream, msgFactory, connection), prime.prime.fixedDelay))

        lazy val defaultAction = ExecuteResponse(Some(PreparedStatementExecution(p, executeRequest.consistency, List(), List())),
          MessageWithDelay(msgFactory.createVoidMessage(stream)))
        matchingPrimedAction.getOrElse(defaultAction)
      case None =>
        statementNotRecognised(stream, msgFactory, executeRequest)
    }
    action
  }

  private def statementNotRecognised(stream: Byte, msgFactory: CqlMessageFactory, executeRequest: ExecuteRequest): ExecuteResponse = {
    val id = serializeInt(executeRequest.id)
    val errMsg = s"Could not find prepared statement with id: ${bytes2Hex(id)}"
    ExecuteResponse(Some(PreparedStatementExecution(errMsg, executeRequest.consistency, List(), List())),
      MessageWithDelay(msgFactory.createErrorMessage(UnpreparedResult(errMsg, id), stream, executeRequest.consistency)))
  }


  private def sendMessage(msgAndDelay: MessageWithDelay, receiver: ActorRef) = {
    msgAndDelay.delay match {
      case None => receiver ! msgAndDelay.msg
      case Some(duration) =>
        log.info(s"Delaying response of prepared statement by $duration")
        context.system.scheduler.scheduleOnce(duration, receiver, msgAndDelay.msg)(context.system.dispatcher)
    }
  }

  private def createMessage(preparedPrime: PreparedPrime, executeRequest: ExecuteRequest ,stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef) = {
    preparedPrime.prime.result match {
      case SuccessResult => msgFactory.createRowsMessage(preparedPrime.prime, stream)
      case result: ErrorResult => msgFactory.createErrorMessage(result, stream, executeRequest.consistency)
      case result: FatalResult => result.produceFatalError(connection)
    }
  }

}

private case class ExecuteResponse(activity: Option[PreparedStatementExecution], msg: MessageWithDelay)
private case class MessageWithDelay(msg: Any, delay: Option[FiniteDuration] = None)

object ExecuteHandler {
  case class Execute(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef)
  private case class DeserializedExecute(request: ExecuteRequest, text: Option[String], body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef)
}
