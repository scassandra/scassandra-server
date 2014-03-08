package uk.co.scassandra.server

import akka.actor.{ActorRef, ActorSystem}
import akka.io.Tcp.Write
import akka.testkit._
import akka.util.ByteString
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import com.batey.narinc.client.cqlmessages.{VoidResult, SetKeyspace}

class QueryHandlerTest extends FunSuite with ShouldMatchers with BeforeAndAfter with TestKitBase {
  implicit lazy val system = ActorSystem()

  var underTest : ActorRef = null
  var testProbe : TestProbe = null

  before {
    testProbe = TestProbe()
    underTest = TestActorRef(new QueryHandler(testProbe.ref))
  }

  test("Should return set keyspace message for use statement") {
    val useStatement: String = "use keyspace"
    val stream: Byte = 0x02
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(useStatement).toArray.drop(8))
 
    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbe.expectMsg(Write(SetKeyspace("keyspace", stream).serialize()))
  }
  
  test("Should return void result for everything apart from use statement") {
    val someCqlStatement: String = "some other cql statement"
    val stream: Byte = 0x05
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement).toArray.drop(8))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery, stream)

    testProbe.expectMsg(Write(VoidResult(stream).serialize()))
  }
}