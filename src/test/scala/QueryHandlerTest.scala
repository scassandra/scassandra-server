import akka.actor.{ActorRef, ActorSystem}
import akka.io.Tcp.Write
import akka.testkit.{TestKitBase, TestKit, TestProbe, TestActorRef}
import akka.util.ByteString
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers

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
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(useStatement).toArray.drop(8))
 
    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery)

    testProbe.expectMsg(Write(SetKeyspace("keyspace").serialize()))
  }
  
  test("Should return void result for everything apart from use statement") {
    val someCqlStatement: String = "some other cql statement"
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(someCqlStatement).toArray.drop(8))

    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery)

    testProbe.expectMsg(Write(VoidResult.serialize()))
  }
}