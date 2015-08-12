package org.scassandra.server.actors


import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.{ActorLogging, ActorRef, Actor}
import akka.util.{Timeout, ByteString}
import akka.pattern.{ask, pipe}

import org.scassandra.server.actors.ExecuteHandler.DeserializedExecute
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementResponse, PreparedStatementQuery}
import org.scassandra.server.cqlmessages.CqlMessageFactory
import org.scassandra.server.cqlmessages.request.ExecuteRequest
import org.scassandra.server.priming._
import org.scassandra.server.priming.prepared.{PreparedPrime, PreparedStoreLookup}
import org.scassandra.server.priming.query.PrimeMatch


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
      val action = handleExecute(body, stream, msgFactory, text, request)
      action.activity.foreach(activityLog.recordPreparedStatementExecution)
      sendMessage(action.msg, connection)
  }

  private def handleExecute(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, prepStatement: Option[String], executeRequest: ExecuteRequest): ExecuteResponse = {
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

  private def statementNotRecognised(stream: Byte, msgFactory: CqlMessageFactory): ExecuteResponse = {
    ExecuteResponse(None, MessageWithDelay(msgFactory.createVoidMessage(stream)))
  }


  private def sendMessage(msgAndDelay: MessageWithDelay, receiver: ActorRef) = {
    msgAndDelay.delay match {
      case None => receiver ! msgAndDelay.msg
      case Some(duration) =>
        log.info(s"Delaying response of prepared statement by $duration")
        context.system.scheduler.scheduleOnce(duration, receiver, msgAndDelay.msg)(context.system.dispatcher)
    }
  }

  private def createMessage(preparedPrime: PreparedPrime, executeRequest: ExecuteRequest ,stream: Byte, msgFactory: CqlMessageFactory) = {
    preparedPrime.prime.result match {
      case SuccessResult => msgFactory.createRowsMessage(preparedPrime.prime, stream)
      case result: ReadRequestTimeoutResult => msgFactory.createReadTimeoutMessage(stream, executeRequest.consistency, result)
      case result: WriteRequestTimeoutResult => msgFactory.createWriteTimeoutMessage(stream, executeRequest.consistency, result)
      case result: UnavailableResult => msgFactory.createUnavailableMessage(stream, executeRequest.consistency, result)
    }
  }

}

private case class ExecuteResponse(activity: Option[PreparedStatementExecution], msg: MessageWithDelay)
private case class MessageWithDelay(msg: Any, delay: Option[FiniteDuration] = None)

object ExecuteHandler {
  case class Execute(body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef)
  private case class DeserializedExecute(request: ExecuteRequest, text: Option[String], body: ByteString, stream: Byte, msgFactory: CqlMessageFactory, connection: ActorRef)
}
