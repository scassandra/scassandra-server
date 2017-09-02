/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
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

import akka.actor.ActorRef
import akka.testkit.{ImplicitSender, TestActorRef, TestProbe}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import org.scassandra.codec._
import org.scassandra.codec.datatype._
import org.scassandra.codec.messages.{ColumnSpecWithoutTable, PreparedMetadata, QueryParameters, Row}
import org.scassandra.server.actors.Activity.PreparedStatementExecution
import org.scassandra.server.actors.ActivityLogActor.RecordExecution
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.actors.priming.PrimePreparedStoreActor.{LookupByExecute, PrimeMatch}
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.Reply
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.language.postfixOps

class ExecuteHandlerTest extends WordSpec with ProtocolActorTest with Matchers with TestKitWithShutdown with ImplicitSender
  with ScalaFutures with BeforeAndAfter {
  implicit val protocolVersion = ProtocolVersion.latest

  var underTest: ActorRef = _
  var prepareHandlerTestProbe: TestProbe = _
  val activityLogProbe = TestProbe()
  val activityLog: ActorRef = activityLogProbe.ref

  val primePreparedStoreProbe = TestProbe()
  val primePreparedStore = primePreparedStoreProbe.ref
  val stream: Byte = 0x3

  implicit val atMost: Duration = 1 seconds
  implicit val timeout: Timeout = 1 seconds

  val preparedId = 1
  val preparedIdBytes = ByteVector(preparedId)

  before {
    prepareHandlerTestProbe = TestProbe()
    underTest = TestActorRef(new ExecuteHandler(primePreparedStore, activityLog, prepareHandlerTestProbe.ref))
    receiveWhile(10 milliseconds) {
      case _ =>
    }
    activityLogProbe.receiveWhile(10 milliseconds) {
      case _ =>
    }
  }

  "execute handler" must {
    "return empty result message for execute if not primed - no params" in {
      val execute = Execute(preparedIdBytes)
      respondWith(primePreparedStoreProbe, PrimeMatch(None))

      underTest ! protocolMessage(execute)

      prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
      prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> ("Some query", Prepared(preparedIdBytes)))))
      primePreparedStoreProbe.expectMsg(LookupByExecute("Some query", execute, protocolVersion))
      expectMsgPF() {
        case ProtocolResponse(_, VoidResult) => true
      }
    }

    "look up prepared prime in store with consistency & query" in {
      val consistency = Consistency.THREE
      val query = "select * from something where name = ?"
      val prepared = Prepared(preparedIdBytes)
      val execute = Execute(preparedIdBytes, QueryParameters(consistency = consistency))
      respondWith(primePreparedStoreProbe, PrimeMatch(None))

      underTest ! protocolMessage(execute)

      prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
      prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> (query, prepared))))
      primePreparedStoreProbe.expectMsg(LookupByExecute(query, execute, protocolVersion))
    }

    "create rows message if prime matches" in {
      val query = "select * from something where name = ?"
      val id = 1
      val rows = Rows(rows = Row("a" -> 1, "b" -> 2) :: Nil)
      val primeMatch = Some(Reply(rows))
      respondWith(primePreparedStoreProbe, PrimeMatch(primeMatch))
      val execute = Execute(preparedIdBytes)

      underTest ! protocolMessage(execute)

      prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
      prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> (query, Prepared(preparedIdBytes)))))
      primePreparedStoreProbe.expectMsg(LookupByExecute(query, execute, protocolVersion))
      expectMsgPF() {
        case ProtocolResponse(_, `rows`) => true
      }
    }

    "record execution in activity log" in {
      val query = "select * from something where name = ?"
      val id = 1
      val rows = Rows(rows = Row("a" -> 1, "b" -> 2) :: Nil)
      val primeMatch = Some(Reply(rows))
      val consistency = Consistency.TWO
      respondWith(primePreparedStoreProbe, PrimeMatch(primeMatch))
      val values = List(10)
      val variableTypes = List(Bigint)
      val execute = Execute(preparedIdBytes, parameters = QueryParameters(consistency,
        values = Some(values.map(v => QueryValue(None, Bytes(Bigint.codec.encode(v).require.bytes))))))

      underTest ! protocolMessage(execute)

      prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
      prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> (query, Prepared(preparedIdBytes,
        preparedMetadata = PreparedMetadata(keyspace = Some("keyspace"), table = Some("table"),
          columnSpec = List(ColumnSpecWithoutTable("0", Bigint))))))))
      activityLogProbe.expectMsg(RecordExecution(PreparedStatementExecution(query, consistency, None, values, variableTypes, None)))
    }

    "record execution in activity log without variables when variables don't match prime" in {
      val query = "select * from something where name = ? and something = ?"
      val consistency = Consistency.TWO
      val primeMatch = Some(Reply(NoRows))
      respondWith(primePreparedStoreProbe, PrimeMatch(primeMatch))

      // Execute statement with two BigInt variables.
      val variables = List(10, 20)
      val execute = Execute(preparedIdBytes, parameters = QueryParameters(consistency = consistency,
        serialConsistency = Some(Consistency.SERIAL),
        values = Some(variables.map(v => QueryValue(None, Bytes(Bigint.codec.encode(v).require.bytes)))),
        timestamp = Some(1000)
      ))

      underTest ! protocolMessage(execute)

      // The Prepared Prime expects 1 column with Varchar.
      prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
      prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> (query, Prepared(preparedIdBytes,
        preparedMetadata = PreparedMetadata(keyspace = Some("keyspace"), table = Some("table"),
          columnSpec = ColumnSpecWithoutTable("0", Varchar) :: Nil))))))

      // The execution should still be recorded, but the variables not included.
      activityLogProbe.expectMsg(RecordExecution(PreparedStatementExecution(query, consistency,
        Some(Consistency.SERIAL), List(), List(), Some(1000))))
    }

    "record execution in activity log event if not primed" in {
      val query = "Some query"
      val consistency = Consistency.TWO
      respondWith(primePreparedStoreProbe, PrimeMatch(None))
      val execute = Execute(preparedIdBytes, QueryParameters(consistency))

      underTest ! protocolMessage(execute)

      prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
      prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> (query, Prepared(preparedIdBytes)))))
      activityLogProbe.expectMsg(RecordExecution(PreparedStatementExecution(query, consistency, None, List(), List(), None)))
    }

    "return unprepared response if not prepared statement not found" in {
      val errMsg = s"Could not find prepared statement with id: 0x${preparedIdBytes.toHex}"
      val consistency = Consistency.TWO
      val execute = Execute(preparedIdBytes, QueryParameters(consistency))

      underTest ! protocolMessage(execute)

      prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
      prepareHandlerTestProbe.reply(PreparedStatementResponse(Map()))
      activityLogProbe.expectMsg(RecordExecution(PreparedStatementExecution(errMsg, consistency, None, List(), List(), None)))
      expectMsgPF() {
        case ProtocolResponse(_, Unprepared(`errMsg`, `preparedIdBytes`)) => true
      }
    }
  }
}
