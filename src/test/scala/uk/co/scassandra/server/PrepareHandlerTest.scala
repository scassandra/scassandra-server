package uk.co.scassandra.server

import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}
import akka.testkit.{TestActorRef, TestProbe, TestKitBase}
import org.scalatest.mock.MockitoSugar
import akka.actor.{ActorRef, ActorSystem}
import org.mockito.Mockito._
import uk.co.scassandra.cqlmessages.response._
import uk.co.scassandra.cqlmessages.{CqlVarchar, VersionTwo, ColumnType, ProtocolVersion}
import akka.util.ByteString
import akka.io.Tcp.Write
import uk.co.scassandra.cqlmessages.request.PrepareRequest
import uk.co.scassandra.cqlmessages.response.PreparedResultV2
import uk.co.scassandra.cqlmessages.request.PrepareRequest
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

class PrepareHandlerTest extends FunSuite with Matchers with TestKitBase with BeforeAndAfter {
  implicit lazy val system = ActorSystem()

  var underTest: ActorRef = null
  var testProbeForTcpConnection: TestProbe = null
  val cqlMessageFactory = VersionTwoMessageFactory
  val protocolVersion: Byte = ProtocolVersion.ServerProtocolVersionTwo
  implicit val impProtocolVersion = VersionTwo

  before {
    testProbeForTcpConnection = TestProbe()
    underTest = TestActorRef(new PrepareHandler())
  }


  test("Should return result prepared message - no params") {
    val stream: Byte = 0x02
    val query = "select * from something"
    val prepareBody: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)

    underTest ! PrepareHandlerMessages.Prepare(prepareBody, stream, cqlMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", Map[String, ColumnType]()))
  }

  test("Should return empty result message for execute - no params") {
    val stream: Byte = 0x02
    val query = "select * from something"
    val prepareBody: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)

    underTest ! PrepareHandlerMessages.Execute(prepareBody, stream, cqlMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(VoidResult(stream))
  }

  test("Should return result prepared message - single param") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ?"
    val prepareBody: ByteString = PrepareRequest(protocolVersion, stream, query).serialize().drop(8)

    underTest ! PrepareHandlerMessages.Prepare(prepareBody, stream, cqlMessageFactory, testProbeForTcpConnection.ref)

    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", Map[String, ColumnType]("0" -> CqlVarchar)))
  }
  
  test("Should use incrementing IDs") {
    underTest = TestActorRef(new PrepareHandler())
    val stream: Byte = 0x02
    val queryOne = "select * from something where name = ?"
    val prepareBodyOne: ByteString = PrepareRequest(protocolVersion, stream, queryOne).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Prepare(prepareBodyOne, stream, cqlMessageFactory, testProbeForTcpConnection.ref)
    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 1.toShort, "keyspace", "table", Map[String, ColumnType]("0" -> CqlVarchar)))

    emptyTestProbe

    val queryTwo = "select * from something where name = ? and age = ?"
    val prepareBodyTwo: ByteString = PrepareRequest(protocolVersion, stream, queryTwo).serialize().drop(8)
    underTest ! PrepareHandlerMessages.Prepare(prepareBodyTwo, stream, cqlMessageFactory, testProbeForTcpConnection.ref)
    testProbeForTcpConnection.expectMsg(PreparedResultV2(stream, 2.toShort, "keyspace", "table", Map[String, ColumnType]("0" -> CqlVarchar, "1" -> CqlVarchar)))

  }

  def emptyTestProbe {
    testProbeForTcpConnection.receiveWhile(idle = Duration(100, TimeUnit.MILLISECONDS))({
      case msg@_ => println(s"Msg left over after test ${msg}")
    })
  }
}
