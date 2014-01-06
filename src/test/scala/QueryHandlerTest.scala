import akka.actor.ActorSystem
import akka.io.Tcp.Write
import akka.testkit.{TestKitBase, TestKit, TestProbe, TestActorRef}
import akka.util.ByteString
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

class QueryHandlerTest extends FunSuite with ShouldMatchers with TestKitBase {
  implicit lazy val system = ActorSystem()

  test("Should return set keyspace message for use statement") {
    val testProbe = TestProbe()
    val underTest = TestActorRef(new QueryHandler(testProbe.ref))
    val useStatement: String = "use keyspace"
    val setKeyspaceQuery: ByteString = ByteString(MessageHelper.createQueryMessage(useStatement).toArray.drop(8))
 
    underTest ! QueryHandlerMessages.Query(setKeyspaceQuery)

    testProbe.expectMsg(Write(SetKeyspace("keyspace").serialize()))
  }
}