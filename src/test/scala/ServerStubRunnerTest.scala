import akka.util.ByteString
import java.io._
import java.net.Socket
import java.net.ConnectException
import java.util
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import scala.collection.immutable.IndexedSeq

class ServerStubRunnerTest extends FunSuite with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {
  var serverThread: Thread = null
  var connectionToServerStub: Socket = null
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  before {
    connectionToServerStub = ConnectionToServerStub()
  }

  after {
    println("Closing socket")
    connectionToServerStub.close()
  }

  test("return nothing until a startup message is received") {
    val bytes = availableBytes(200)

    bytes should equal(0)

    sendStartupMessage()

    val bytesAfterStartupMessage = availableBytes(200)
    bytesAfterStartupMessage should equal(8)
  }

  test("return nothing if an options message is received") {
    val bytes = availableBytes(200)
    bytes should equal(0)

    sendOptionsMessage

    val bytesAfterOptionsMessage = availableBytes(200)
    bytesAfterOptionsMessage should equal(0)
  }

  test("return a version byte in the response header") {
    sendStartupMessage()

    val in = new DataInputStream(connectionToServerStub.getInputStream)

    // read first byte
    val responseHeaderVersion: Int = in.read()

    val responseHeaderVersionTwo = 0x82
    responseHeaderVersion should equal(responseHeaderVersionTwo)
  }

  test("return a flags byte in the response header with all bits set to 0 on STARTUP request") {
    sendStartupMessage()

    val in = new DataInputStream(connectionToServerStub.getInputStream)

    // consume first byte
    consumeBytes(in, 1)

    // read second byte
    val responseHeaderFlags: Int = in.read()

    val noCompressionFlags = 0x00
    responseHeaderFlags should equal(noCompressionFlags)
  }

  test("return a stream byte in the response header") {
    implicit val in = new DataInputStream(connectionToServerStub.getInputStream)
    sendStartupMessage()

    // consume first two bytes
    consumeBytes(in, 2)

    // read third byte
    val responseHeaderStream: Int = in.read()

    // TODO: in future versions, the stream ID should reflect the value set in the request header
    val noStreamId = 0x00
    responseHeaderStream should equal(noStreamId)
  }

  test("return a READY opCode byte in the response header on STARTUP request") {
    implicit val in = new DataInputStream(connectionToServerStub.getInputStream)
    sendStartupMessage()

    val responseHeaderOpCode: Int = readResponseHeaderOpCode

    responseHeaderOpCode should equal(OpCodes.Ready)
  }

  test("return length field with all 4 bytes set to 0 on STARTUP request") {
    sendStartupMessage()

    val in = new DataInputStream(connectionToServerStub.getInputStream)

    // consume first four bytes
    consumeBytes(in, 4)

    // read next four bytes
    var length = 0
    for (a <- 1 to 4) {
      length += in.read()
    }

    length should equal(0)
  }

  test("return RESULT OpCode on Query") {
    implicit val stream: OutputStream = connectionToServerStub.getOutputStream
    implicit val inputStream : DataInputStream = new DataInputStream(connectionToServerStub.getInputStream)

    sendStartupMessage()
    readReadyMessage()

    val queryAsBytes = "select * from people"
    sendQueryMessage(queryAsBytes)

    val responseHeaderOpCode: Int = readResponseHeaderOpCode

    responseHeaderOpCode should equal(OpCodes.Result)
  }

  test("should reject query message if startup message has not been sent") {
    val in = new DataInputStream(connectionToServerStub.getInputStream)
    sendQueryMessage()
    
    // consume first three bytes
    consumeBytes(in, 3)

    // read fourth byte
    val responseHeaderOpCode: Int = in.read()

    responseHeaderOpCode should equal(OpCodes.Error)
  }

  test("test receiving size separately from opcode") {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, 0x00, OpCodes.Startup))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x00, 0x00, 0x00, 0x0)) // empty body
    stream.flush()

    readReadyMessage()
  }

  test("test receiving body separately from opcode and size") {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, 0x00, OpCodes.Startup, 0x00, 0x00, 0x00, 0x16))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x0,0x1,0x0,0xb,0x43,0x51,0x4c,0x5f,0x56,0x45,0x52,0x53,0x49,0x4f,0x4e,0x00,0x05,0x33,0x2e,30,0x2e,30))
    stream.flush()

    readReadyMessage()
  }

  test("test receiving start of header separately to opcode") {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x00, OpCodes.Startup, 0x00, 0x00, 0x00, 0x0)) // empty body
    stream.flush()

    readReadyMessage()
  }

  test("test receiving message in three parts") {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, 0x00, OpCodes.Startup, 0x00, 0x00, 0x00, 0x16))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x0,0x1,0x0,0xb,0x43,0x51))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x4c,0x5f,0x56,0x45,0x52,0x53,0x49,0x4f,0x4e,0x00,0x05,0x33,0x2e,30,0x2e,30))

    readReadyMessage()
  }

  test("test receiving whole messages after partial messages") {
    implicit val stream: OutputStream = connectionToServerStub.getOutputStream
    implicit val inputStream : DataInputStream = new DataInputStream(connectionToServerStub.getInputStream)

    stream.write(Array[Byte](0x02, 0x00, 0x00, OpCodes.Startup, 0x00, 0x00, 0x00, 0x16))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x0,0x1,0x0,0xb,0x43,0x51))
    stream.flush()
    Thread.sleep(500)
    stream.write(Array[Byte](0x4c,0x5f,0x56,0x45,0x52,0x53,0x49,0x4f,0x4e,0x00,0x05,0x33,0x2e,30,0x2e,30))

    readReadyMessage()
    sendQueryMessage("select * from people")
    val responseHeaderOpCode: Int = readResponseHeaderOpCode
    responseHeaderOpCode should equal(OpCodes.Result)
  }

  test("should return a result message with keyspace name on use statement") {
    implicit val in = new DataInputStream(connectionToServerStub.getInputStream)
    implicit val stream: OutputStream = connectionToServerStub.getOutputStream

    sendStartupMessage()
    readReadyMessage()

    sendQueryMessage("use people")

    val responseOpCode = readResponseHeaderOpCode()
    responseOpCode should equal(OpCodes.Result)

    val message = readMessageBody()
    println(s"Message body received ${util.Arrays.toString(message)}")

    val responseType = takeInt(message)
    responseType shouldEqual(ResultTypes.SetKeyspace)

    val cqlString = takeString(message.drop(4))
    cqlString.trim should equal("people")
  }

  def takeInt(bytes : Array[Byte]) = {
    ByteString(bytes.take(4)).asByteBuffer.getInt
  }

  def takeString(bytes : Array[Byte]) = {
    val stringLength = ByteString(bytes.take(2)).asByteBuffer.getShort
    new String(bytes.drop(2).take(stringLength))
  }

  def readMessageBody()(implicit inputStream : DataInputStream) = {
    // read the length
    val length = readInteger()
    println(s"Read length ${length}")
    // read the rest
    val messageBody = new Array[Byte](length)
    inputStream.read(messageBody)
    messageBody
  }

  override def beforeAll {
    // First ensure nothing else is running on port 9042
    var somethingAlreadyRunning = true

    try {
      ConnectionToServerStub()
    } catch {
      case ce: ConnectException => {
        somethingAlreadyRunning = false
      }
    }

    if (somethingAlreadyRunning) {
      fail("There must not be any server already running")
    }

    // Then start the server
    startServerStub()
  }

  override def afterAll {
    stopServerStub()
  }

  def readReadyMessage() = {
    val stream: DataInputStream = new DataInputStream(connectionToServerStub.getInputStream)
    consumeBytes(stream, 8)
  }

  def readResponseHeaderOpCode()(implicit inputStream : DataInputStream) = {
    // consume first three bytes
    consumeBytes(inputStream, 3)

    // read fourth byte
    inputStream.read()
  }

  def availableBytes(timeToWaitMillis: Long): Int = {
    // TODO: Make this check every N millis rather than wait the full amount first?
    Thread.sleep(timeToWaitMillis)
    val stream = new DataInputStream(connectionToServerStub.getInputStream)
    stream.available()
  }

  //TODO: Make this timeout
  def consumeBytes(stream: DataInputStream, numberOfBytes: Int) {
    for (i <- 1 to numberOfBytes) {
      stream.read()
    }
  }

  def startServerStub() = {
    serverThread = ServerStubAsThread()
    serverThread.start()
    Thread.sleep(5000)
  }

  def stopServerStub() = {
    ServerStubRunner.shutdown()
  }

  def sendStartupMessage() = {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, 0x00, OpCodes.Startup))
    stream.write(Array[Byte](0x00, 0x00, 0x00, 0x16))
    val fakeBody: IndexedSeq[Byte] = for (i <- 0 until 22) yield 0x00.toByte
    stream.write(fakeBody.toArray)
    stream.flush()
  }

  def sendQueryMessage(queryString : String = "select * from people") = {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    val queryMessage = MessageHelper.createQueryMessage(queryString)
    stream.write(queryMessage.toArray)
  }


  def sendOptionsMessage {
    val stream: OutputStream = connectionToServerStub.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, 0x00, OpCodes.Options))
    sendFakeLengthAndBody(stream)
  }

  def sendFakeLengthAndBody(stream: OutputStream) {
    stream.write(Array[Byte](0x00, 0x00, 0x00, 0x16))
    val fakeBody: IndexedSeq[Byte] = for (i <- 0 until 22) yield 0x00.toByte
    stream.write(fakeBody.toArray)
  }

  def readInteger()(implicit inputStream : DataInputStream) = {
    val bytes = new Array[Byte](4)
    inputStream.read(bytes)
    val byteString = ByteString(bytes)
    byteString.toByteBuffer.getInt
  }

}

object ConnectionToServerStub {
  val ServerHost = "localhost"
  val ServerPort = 8042

  def apply() = {
    val socket = new Socket(ServerHost, ServerPort)
    socket.setSoTimeout(1000)
    socket
  }
}

object ServerStubAsThread {
  def apply() = new Thread(new Runnable {
    def run() {
      ServerStubRunner.run()
    }
  })
}