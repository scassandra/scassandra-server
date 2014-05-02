package uk.co.scassandra.server

import akka.actor.ActorSystem
import akka.io.Tcp.{Received, Write}
import akka.testkit._
import akka.util.ByteString
import org.scalatest._
import uk.co.scassandra.server.QueryHandlerMessages.Query
import uk.co.scassandra.cqlmessages._
import uk.co.scassandra.cqlmessages.response.{QueryBeforeReadyMessage, Ready}
import uk.co.scassandra.cqlmessages.response.{CqlMessageFactory, VersionTwoMessageFactory, VersionOneMessageFactory}
import uk.co.scassandra.cqlmessages.response.QueryBeforeReadyMessage
import akka.io.Tcp.Received
import uk.co.scassandra.cqlmessages.response.Ready
import uk.co.scassandra.server.QueryHandlerMessages.Query

class ConnectionHandlerTest extends TestKit(ActorSystem("Test")) with Matchers with ImplicitSender with FunSuiteLike with BeforeAndAfter {

  var testActorRef : TestActorRef[ConnectionHandler] = null

  var queryHandlerTestProbe : TestProbe = null
  var registerHandlerTestProbe : TestProbe = null
  var prepareHandlerTestProbe : TestProbe = null
  var tcpWrapperTestProbe : TestProbe = null

  var lastMsgFactoryUsedForQuery : CqlMessageFactory = null
  var lastMsgFactoryUsedForRegister : CqlMessageFactory = null
  var lastMsgFactoryUsedForPrepare : CqlMessageFactory = null

  before {
    tcpWrapperTestProbe = TestProbe()
    queryHandlerTestProbe = TestProbe()
    registerHandlerTestProbe = TestProbe()
    prepareHandlerTestProbe = TestProbe()
    testActorRef = TestActorRef(new ConnectionHandler(
      (_,_,msgFactory) => {
        lastMsgFactoryUsedForQuery = msgFactory
        queryHandlerTestProbe.ref
      },
      (_,_,msgFactory) => {
        lastMsgFactoryUsedForRegister = msgFactory
        registerHandlerTestProbe.ref
      },
      prepareHandlerTestProbe.ref,
      (_,_) => {
        tcpWrapperTestProbe.ref
      }
    ))

    lastMsgFactoryUsedForQuery = null
  }

  test("Should do nothing if not a full message") {
    val partialMessage = ByteString(
      Array[Byte](
        ProtocolVersion.ServerProtocolVersionTwo, 0x0, 0x0, OpCodes.Query, // header
        0x0, 0x0, 0x0, 0x5,  // length
        0x0 // 4 bytes missing
      )
    )

    testActorRef ! Received(partialMessage)

    queryHandlerTestProbe.expectNoMsg()
  }

  test("Should send ready message when startup message sent - version one") {
    implicit val protocolVersion = VersionOne
    val readyMessage = ByteString(
      Array[Byte](
        ProtocolVersion.ClientProtocolVersionOne, 0x0, 0x0, OpCodes.Startup, // header
        0x0, 0x0, 0x0, 0x0 // length
      )
    )

    testActorRef ! Received(readyMessage)

    expectMsg(Write(Ready(0x0.toByte).serialize()))
  }

  test("Should send ready message when startup message sent - version two") {
    implicit val protocolVersion = VersionTwo
    val readyMessage = ByteString(
      Array[Byte](
        ProtocolVersion.ClientProtocolVersionTwo, 0x0, 0x0, OpCodes.Startup, // header
        0x0, 0x0, 0x0, 0x0 // length
      )
    )

    testActorRef ! Received(readyMessage)

    expectMsg(Write(Ready(0x0.toByte).serialize()))
  }

  test("Should send back error if query before ready message") {
    implicit val protocolVersion = VersionTwo
    val queryMessage = ByteString(
      Array[Byte](
        protocolVersion.clientCode, 0x0, 0x0, OpCodes.Query, // header
        0x0, 0x0, 0x0, 0x0 // length
      )
    )

    testActorRef ! Received(queryMessage)

    expectMsg(Write(QueryBeforeReadyMessage().serialize()))
  }

  test("Should do nothing if an unrecognised opcode") {
    val unrecognisedOpCode = ByteString(
      Array[Byte](
        ProtocolVersion.ServerProtocolVersionTwo  , 0x0, 0x0, 0x56, // header
        0x0, 0x0, 0x0, 0x0 // length
      )
    )

    testActorRef ! Received(unrecognisedOpCode)

    expectNoMsg()
    queryHandlerTestProbe.expectNoMsg()
  }


  test("Should forward query to a new QueryHandler - version two of protocol") {
    sendStartupMessage()
    val stream : Byte = 0x04
    val query = "select * from people"
    val queryLength = Array[Byte](0x0, 0x0, 0x0, query.length.toByte)
    val queryOptions = Array[Byte](0,1,0)
    val queryWithLengthAndOptions = queryLength ++ query.getBytes() ++ queryOptions
    val queryMessage = MessageHelper.createQueryMessage(query, stream, protocolVersion = ProtocolVersion.ClientProtocolVersionTwo)

    testActorRef ! Received(ByteString(queryMessage.toArray))

    queryHandlerTestProbe.expectMsg(Query(ByteString(queryWithLengthAndOptions), stream))
    lastMsgFactoryUsedForQuery should equal(VersionTwoMessageFactory)
  }
  test("Should forward query to a new QueryHandler - version one of protocol") {
    sendStartupMessage()
    val stream : Byte = 0x04
    val query = "select * from people"
    val queryLength = Array[Byte](0x0, 0x0, 0x0, query.length.toByte)
    val queryOptions = Array[Byte](0,1,0)
    val queryWithLengthAndOptions = queryLength ++ query.getBytes() ++ queryOptions
    val queryMessage = MessageHelper.createQueryMessage(query, stream, protocolVersion = ProtocolVersion.ClientProtocolVersionOne)

    testActorRef ! Received(ByteString(queryMessage.toArray))

    queryHandlerTestProbe.expectMsg(Query(ByteString(queryWithLengthAndOptions), stream))
    lastMsgFactoryUsedForQuery should equal(VersionOneMessageFactory)
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

  test("Should forward register message to RegisterHandler - version two protocol") {
    sendStartupMessage()

    val registerMessage = MessageHelper.createRegisterMessage(ProtocolVersion.ClientProtocolVersionTwo)

    testActorRef ! Received(ByteString(registerMessage.toArray))

    registerHandlerTestProbe.expectMsg(RegisterHandlerMessages.Register(ByteString(MessageHelper.dropHeaderAndLength(registerMessage.toArray))))
    lastMsgFactoryUsedForRegister should equal(VersionTwoMessageFactory)
  }

  test("Should forward register message to RegisterHandler - version one protocol") {
    sendStartupMessage()

    val registerMessage = MessageHelper.createRegisterMessage(ProtocolVersion.ClientProtocolVersionOne)

    testActorRef ! Received(ByteString(registerMessage.toArray))

    registerHandlerTestProbe.expectMsg(RegisterHandlerMessages.Register(ByteString(MessageHelper.dropHeaderAndLength(registerMessage.toArray))))
    lastMsgFactoryUsedForRegister should equal(VersionOneMessageFactory)
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

  test("Should forward Preprare messages to the prepare handler") {
    sendStartupMessage()
    val streamId : Byte = 0x1
    val headerForPrepareMessage = new Header(ProtocolVersion.ClientProtocolVersionTwo,
                                             OpCodes.Prepare, streamId)
    val emptyPrepareMessage = headerForPrepareMessage.serialize() ++ Array[Byte](0,0,0,0)
    
    testActorRef ! Received(ByteString(emptyPrepareMessage))

    prepareHandlerTestProbe.expectMsg(PrepareHandlerMessages.Prepare(ByteString(), streamId, VersionTwoMessageFactory, tcpWrapperTestProbe.ref))
  }

  test("Should forward Execute messages to the prepare handler") {
    sendStartupMessage()
    val streamId : Byte = 0x1
    val headerForPrepareMessage = new Header(ProtocolVersion.ClientProtocolVersionTwo,
      OpCodes.Execute, streamId)
    val messageBody = Array[Byte](5,6)
    val emptyPrepareMessage = headerForPrepareMessage.serialize() ++
      Array[Byte](0,0,0,messageBody.length.toByte) ++ messageBody

    testActorRef ! Received(ByteString(emptyPrepareMessage))

    prepareHandlerTestProbe.expectMsg(PrepareHandlerMessages.Execute(ByteString(messageBody), streamId, VersionTwoMessageFactory, tcpWrapperTestProbe.ref))
  }

  private def sendStartupMessage() = {
    val startupMessage = MessageHelper.createStartupMessage()
    testActorRef ! Received(ByteString(startupMessage.toArray))
  }

}
