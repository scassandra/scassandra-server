package uk.co.scassandra.server

import akka.actor.ActorSystem
import akka.io.Tcp.{Received, Write}
import akka.testkit._
import akka.util.ByteString
import org.scalatest._
import uk.co.scassandra.server.QueryHandlerMessages.Query
import org.scassandra.cqlmessages.{HeaderConsts, OpCodes}
import org.scassandra.cqlmessages.response.{QueryBeforeReadyMessage, Ready}

class ConnectionHandlerTest extends TestKit(ActorSystem("Test")) with Matchers with ImplicitSender with FunSuiteLike with BeforeAndAfter {

  var queryHandlerTestProbe : TestProbe = null
  var registerHandlerTestProbe : TestProbe = null
  var testActorRef : TestActorRef[ConnectionHandler] = null
  
  before {
    queryHandlerTestProbe = TestProbe()
    registerHandlerTestProbe = TestProbe()
    testActorRef = TestActorRef(new ConnectionHandler(
      (_, _) => queryHandlerTestProbe.ref,
      (_, _) => registerHandlerTestProbe.ref))
  }
  
  test("Should do nothing if not a full message") {
    val partialMessage = ByteString(
      Array[Byte](
        HeaderConsts.ServerProtocolVersion, 0x0, 0x0, OpCodes.Query, // header
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
        HeaderConsts.ServerProtocolVersion, 0x0, 0x0, OpCodes.Startup, // header
        0x0, 0x0, 0x0, 0x0 // length
      )
    )

    testActorRef ! Received(readyMessage)

    expectMsg(Write(Ready().serialize()))
  }

  test("Should send back error if query before ready message") {
    val queryMessage = ByteString(
      Array[Byte](
        HeaderConsts.ServerProtocolVersion, 0x0, 0x0, OpCodes.Query, // header
        0x0, 0x0, 0x0, 0x0 // length
      )
    )

    testActorRef ! Received(queryMessage)

    expectMsg(Write(QueryBeforeReadyMessage().serialize()))
  }

  test("Should do nothing if an unrecognised opcode") {
    val unrecognisedOpCode = ByteString(
      Array[Byte](
        HeaderConsts.ServerProtocolVersion  , 0x0, 0x0, 0x56, // header
        0x0, 0x0, 0x0, 0x0 // length
      )
    )

    testActorRef ! Received(unrecognisedOpCode)

    expectNoMsg()
    queryHandlerTestProbe.expectNoMsg()
  }


  test("Should forward query to a new uk.co.scassandra.server.co.uk.scassandra.uk.co.scassandra.server.QueryHandler") {
    sendStartupMessage()
    val stream : Byte = 0x04
    val query = "select * from people"
    val queryLength = Array[Byte](0x0, 0x0, 0x0, query.length.toByte)
    val queryOptions = Array[Byte](0,1,0)
    val queryWithLengthAndOptions = queryLength ++ query.getBytes() ++ queryOptions
    val queryMessage = MessageHelper.createQueryMessage(query, stream)

    testActorRef ! Received(ByteString(queryMessage.toArray))

    queryHandlerTestProbe.expectMsg(Query(ByteString(queryWithLengthAndOptions), stream))
  }

  test("Should handle query message coming in two parts") {
    sendStartupMessage()
    val query = "select * from people"
    val stream : Byte = 0x05
    val queryLength = Array[Byte](0x0, 0x0, 0x0, query.length.toByte)
    val queryOptions = Array[Byte](0,1,0)
    val queryWithLengthAndOptions = queryLength ++ query.getBytes() ++ queryOptions
    val queryMessage = MessageHelper.createQueryMessage(query, stream)
    
    val queryMessageFirstHalf = queryMessage take 5 toArray
    val queryMessageSecondHalf = queryMessage drop 5 toArray

    testActorRef ! Received(ByteString(queryMessageFirstHalf))
    queryHandlerTestProbe.expectNoMsg()
    
    testActorRef ! Received(ByteString(queryMessageSecondHalf))
    queryHandlerTestProbe.expectMsg(Query(ByteString(queryWithLengthAndOptions), stream))
  }

  test("Should forward register message to uk.co.scassandra.server.co.uk.scassandra.uk.co.scassandra.server.RegisterHandler") {
    sendStartupMessage()

    val registerMessage = MessageHelper.createRegisterMessage()

    testActorRef ! Received(ByteString(registerMessage.toArray))

    registerHandlerTestProbe.expectMsg(RegisterHandlerMessages.Register(ByteString(MessageHelper.dropHeaderAndLength(registerMessage.toArray))))
  }

  test("Should handle two cql messages in the same data message") {
    val startupMessage = MessageHelper.createStartupMessage()
    val stream : Byte = 0x04
    val query = "select * from people"
    val queryLength = Array[Byte](0x0, 0x0, 0x0, query.length.toByte)
    val queryOptions = Array[Byte](0,1,0)
    val queryWithLengthAndOptions = queryLength ++ query.getBytes() ++ queryOptions
    val queryMessage = MessageHelper.createQueryMessage(query, stream)

    val twoMessages: List[Byte] = startupMessage ++ queryMessage

    testActorRef ! Received(ByteString(twoMessages.toArray))

    queryHandlerTestProbe.expectMsg(Query(ByteString(queryWithLengthAndOptions), stream))
  }

  private def sendStartupMessage() = {
    val startupMessage = MessageHelper.createStartupMessage()
    testActorRef ! Received(ByteString(startupMessage.toArray))
  }

}
