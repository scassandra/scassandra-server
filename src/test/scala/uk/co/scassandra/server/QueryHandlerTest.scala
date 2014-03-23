package uk.co.scassandra.server

import akka.actor.{ActorRef, ActorSystem}
import akka.io.Tcp.Write
import akka.testkit._
import akka.util.ByteString
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import uk.co.scassandra.priming._
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.batey.narinc.client.cqlmessages.response._
import scala.Some
import com.batey.narinc.client.cqlmessages.response.ReadRequestTimeout
import com.batey.narinc.client.cqlmessages.response.VoidResult
import com.batey.narinc.client.cqlmessages.response.Row
import com.batey.narinc.client.cqlmessages.response.SetKeyspace
import com.batey.narinc.client.cqlmessages.response.UnavailableException
import com.batey.narinc.client.cqlmessages.response.Rows
import scala.Some
import uk.co.scassandra.priming.Prime

class QueryHandlerTest extends FunSuite with ShouldMatchers with BeforeAndAfter with TestKitBase with MockitoSugar {
  implicit lazy val system = ActorSystem()

  var underTest : ActorRef = null
  var testProbeForTcpConnection : TestProbe = null
  val mockPrimedResults = mock[PrimedResults]

  before {
    testProbeForTcpConnection = TestProbe()
    underTest = TestActorRef(new QueryHandler(testProbeForTcpConnection.ref, mockPrimedResults))
    reset(mockPrimedResults)
  }

  test("Should return set keyspace message for use statement") {
    val useStatement: String = "use keyspace"
    val stream: Byte = 0x02
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(useStatement).toArray.drop(8))
 
    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(SetKeyspace("keyspace", stream).serialize()))
  }
  
  test("Should return void result for everything that PrimedResults returns None") {
    val someCqlStatement: String = "some other cql statement"
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement).toArray.drop(8))
    when(mockPrimedResults.get(anyString())).thenReturn(None)

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(VoidResult(stream).serialize()))
  }

  test("Should return empty rows result for when PrimedResults returns empty list") {
    val someCqlStatement: String = "some other cql statement"
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement).toArray.drop(8))
    when(mockPrimedResults.get(anyString())).thenReturn(Some(Prime(someCqlStatement, List())))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(Rows("", "", stream, List()).serialize()))
  }

  test("Should return rows result for when PrimedResults returns a list of rows") {
    val someCqlStatement: String = "some other cql statement"
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement).toArray.drop(8))
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(someCqlStatement, List[Map[String, String]](
      Map(
        "name" -> "Mickey",
        "age" -> "99"
      )
    ))))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(Rows("", "", stream, List("name", "age"), List(
      Row(Map(
        "name" -> "Mickey",
        "age" -> "99"
      ))
    )).serialize()))
  }

  test("Should return ReadTimeout if Metadata result is Readtimeout") {
    val someCqlStatement: String = "some other cql statement"
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement).toArray.drop(8))
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(someCqlStatement, List(), ReadTimeout)))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(ReadRequestTimeout(stream).serialize()))
  }

  test("Should return Unavailable Exception if Metadata result is UnavailableException") {
    val someCqlStatement: String = "some other cql statement"
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement).toArray.drop(8))
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(someCqlStatement, List(), Unavailable)))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(UnavailableException(stream).serialize()))
  }

  test("Test multiple rows") {
    val someCqlStatement: String = "some other cql statement"
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement).toArray.drop(8))
    val rows = List[Map[String, String]](
      Map(
        "name" -> "Mickey",
        "age" -> "99"
      ),
      Map(
        "name" -> "Jenifer",
        "age" -> "88"
      )
    )
    when(mockPrimedResults.get(someCqlStatement)).thenReturn(Some(Prime(someCqlStatement, rows)))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbeForTcpConnection.expectMsg(Write(Rows("", "", stream, List("name", "age"),
      rows.map(row => Row(row))).serialize()))
  }


  test("Should store query in the ActivityLog") {
    //given
    ActivityLog.clearQueries()
    val stream : Byte = 1
    val query = "select * from people"
    val queryBody: ByteString = ByteString(MessageHelper.createQueryMessage(query).toArray.drop(8))

    //when
    underTest ! QueryHandlerMessages.Query(queryBody, stream)

    //then
    ActivityLog.retrieveQueries().exists(p => p.query == query) should equal(true)
  }

}