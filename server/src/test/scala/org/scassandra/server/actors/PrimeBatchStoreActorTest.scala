package org.scassandra.server.actors

import akka.actor.Props
import akka.testkit.ImplicitSender
import org.scalatest.WordSpec
import org.scassandra.codec.Consistency
import org.scassandra.codec.Consistency._
import org.scassandra.codec.messages.BatchQueryKind._
import org.scassandra.codec.messages.BatchType
import org.scassandra.codec.messages.BatchType._
import org.scassandra.server.actors.Activity.{BatchExecution, BatchQuery}
import org.scassandra.server.actors.PrimeBatchStoreActor._
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query.Then

class PrimeBatchStoreActorTest extends WordSpec with TestKitWithShutdown with ImplicitSender {
  val primeRequest = BatchPrimeSingle(BatchWhen(List(BatchQueryPrime("select * blah", Simple)), consistency = Some(List(ONE))), Then(result = Some(Success)))
  private val matchingQueries = Seq(BatchQuery("select * blah", Simple))
  val matchingExecution = BatchExecution(matchingQueries, ONE, Some(Consistency.SERIAL), LOGGED, None)

  "a prime batch store" must {
    val batchStore = system.actorOf(Props[PrimeBatchStoreActor])
    batchStore ! RecordBatchPrime(primeRequest)

    "match single batches" in {
      batchStore ! MatchBatch(matchingExecution)
      expectMsg(MatchResult(Some(primeRequest.prime)))
    }

    "not match if queries are different" in {
      val differentQueries = matchingQueries.map(bq => bq.copy(query = "Different"))
      batchStore ! MatchBatch(matchingExecution.copy(batchQueries = differentQueries))
      expectMsg(MatchResult(None))
    }

    "not match if consistency is different" in {
      batchStore ! MatchBatch(matchingExecution.copy(consistency = Consistency.ALL))
      expectMsg(MatchResult(None))
    }

    "not match if batch type is different" in {
      batchStore ! MatchBatch(matchingExecution.copy(batchType = BatchType.COUNTER))
      expectMsg(MatchResult(None))
    }

    "allow clearing" in {
      batchStore ! ClearPrimes
      batchStore ! MatchBatch(matchingExecution)
      expectMsg(MatchResult(None))
    }
  }
}
