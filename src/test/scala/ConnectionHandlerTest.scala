import akka.actor.ActorSystem
import akka.io.Tcp.{Received, Write}
import akka.testkit._
import akka.util.ByteString
import org.scalatest.{BeforeAndAfter, FunSuiteLike}
import org.scalatest.matchers.ShouldMatchers

class ConnectionHandlerTest extends TestKit(ActorSystem("Test")) with ShouldMatchers with ImplicitSender with FunSuiteLike with BeforeAndAfter {

  var queryHandlerTestProbe : TestProbe = null
  var testActorRef : TestActorRef[ConnectionHandler] = null
  
  before {
    queryHandlerTestProbe = TestProbe()
    testActorRef = TestActorRef(new ConnectionHandler((factory, sender) => queryHandlerTestProbe.ref))
  }
  
  test("Should do nothing if not a full message") {
    val partialMessage = ByteString(
      Array[Byte](
        HeaderConsts.ProtocolVersion, 0x0, 0x0, OpCodes.Query, // header
        0x0, 0x0, 0x0, 0x5,  // length
        0x0 // 4 bytes missing
      )
    )

    testActorRef ! Received(partialMessage)

    queryHandlerTestProbe.expectNoMsg()
  }

  test("Should send ready message when startup message sent") {
    val readyMessage = ByteString(
      Array[Byte](
        HeaderConsts.ProtocolVersion, 0x0, 0x0, OpCodes.Startup, // header
        0x0, 0x0, 0x0, 0x0 // length
      )
    )

    testActorRef ! Received(readyMessage)

    expectMsg(Write(Ready.serialize()))
  }

  test("Should send back error if query before ready message") {
    val queryMessage = ByteString(
      Array[Byte](
        HeaderConsts.ProtocolVersion, 0x0, 0x0, OpCodes.Query, // header
        0x0, 0x0, 0x0, 0x0 // length
      )
    )

    testActorRef ! Received(queryMessage)

    expectMsg(Write(QueryBeforeReadyMessage.serialize()))
  }

  test("Should do nothing if an unrecognised opcode") {
    val unrecognisedOpCode = ByteString(
      Array[Byte](
        HeaderConsts.ProtocolVersion, 0x0, 0x0, 0x56, // header
        0x0, 0x0, 0x0, 0x0 // length
      )
    )

    testActorRef ! Received(unrecognisedOpCode)

    expectNoMsg()
    queryHandlerTestProbe.expectNoMsg()
  }
}
