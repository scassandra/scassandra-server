package org.scassandra.server.actors

import akka.actor.Actor
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.BatchQueryKind.BatchQueryKind
import org.scassandra.codec.messages.BatchType.BatchType
import org.scassandra.server.actors.Activity._
import org.scassandra.server.actors.ActivityLogActor._

class ActivityLogActor extends Actor {

  import context._

  def receive = activity(ActivityLog())

  def activity(ac: ActivityLog): Receive = {
    case GetAllQueries => sender ! Queries(ac.queries.reverse)
    case GetAllConnections => sender ! Connections(ac.connections.reverse)
    case GetAllBatches => sender ! Batches(ac.batches.reverse)
    case GetAllExecutions => sender ! Executions(ac.executions.reverse)
    case GetAllPrepares => sender ! Prepares(ac.prepares.reverse)

    case RecordConnection() =>
      become(activity(ac.copy(connections = Connection() :: ac.connections)))
    case RecordQuery(query) =>
      become(activity(ac.copy(queries = query :: ac.queries)))
    case RecordPrepare(prepare) =>
      become(activity(ac.copy(prepares = prepare :: ac.prepares)))
    case RecordExecution(execution) =>
      become(activity(ac.copy(executions = execution :: ac.executions)))
    case RecordBatch(batch) =>
      become(activity(ac.copy(batches = batch :: ac.batches)))

    case ClearConnections =>
      become(activity(ac.copy(connections = List())))
    case ClearQueries =>
      become(activity(ac.copy(queries = List())))
    case ClearBatches =>
      become(activity(ac.copy(batches = List())))
    case ClearPrepares =>
      become(activity(ac.copy(prepares = List())))
    case ClearExecutions =>
      become(activity(ac.copy(executions = List())))
    case ClearAll =>
      become(activity(ActivityLog()))
  }
}

object ActivityLogActor {

  case object GetAllQueries
  case object GetAllConnections
  case object GetAllPrepares
  case object GetAllExecutions
  case object GetAllBatches
  case object ClearAll

  case object ClearConnections
  case object ClearQueries
  case object ClearPrepares
  case object ClearExecutions
  case object ClearBatches

  case class RecordConnection()
  case class RecordQuery(query: Query)
  case class RecordPrepare(prepare: PreparedStatementPreparation)
  case class RecordExecution(prepare: PreparedStatementExecution)
  case class RecordBatch(batch: BatchExecution)

  private case class ActivityLog(
    queries: List[Query] = List(),
    connections: List[Connection] = List(),
    prepares: List[PreparedStatementPreparation] = List(),
    executions: List[PreparedStatementExecution] = List(),
    batches: List[BatchExecution] = List())

  case class Queries(list: List[Query])
  case class Connections(list: List[Connection])
  case class Prepares(list: List[PreparedStatementPreparation])
  case class Executions(list: List[PreparedStatementExecution])
  case class Batches(list: List[BatchExecution])

}

object Activity {
  case class Query(query: String, consistency: Consistency, serialConsistency: Option[Consistency],
    variables: List[Any] = List(), variableTypes: List[DataType] = List(), timestamp: Option[Long] = None)
  case class Connection(result: String = "success")
  case class PreparedStatementExecution(preparedStatementText: String, consistency: Consistency,
    serialConsistency: Option[Consistency], variables: List[Any],
    variableTypes: List[DataType], timestamp: Option[Long])
  case class BatchQuery(query: String, batchQueryKind: BatchQueryKind, variables: List[Any] = List(), variableTypes: List[DataType] = List())
  case class BatchExecution(batchQueries: Seq[BatchQuery], consistency: Consistency,
    serialConsistency: Option[Consistency], batchType: BatchType, timestamp: Option[Long])
  case class PreparedStatementPreparation(preparedStatementText: String)
}
