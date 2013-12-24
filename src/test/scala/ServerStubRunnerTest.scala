import akka.util.ByteString
import java.io._
import java.net.Socket
import java.net.ConnectException
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

    val queryAsBytes = "select * from people".toCharArray.map(_.toByte)
    sendQuery(queryAsBytes)

    val responseHeaderOpCode: Int = readResponseHeaderOpCode

    responseHeaderOpCode should equal(OpCodes.Result)
  }

  test("should reject query message if startup message has not been sent") {
    sendQueryMessage()

    val in = new DataInputStream(connectionToServerStub.getInputStream)

    // consume first three bytes
    consumeBytes(in, 3)

    // read fourth byte
    val responseHeaderOpCode: Int = in.read()

    responseHeaderOpCode should equal(OpCodes.Error)
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
  }

  def sendQueryMessage(queryString : String = "select * from people") = {
    val stream: OutputStream = connectionToServerStub.getOutputStream

    stream.write(Array[Byte](0x02, 0x00, 0x00, OpCodes.Query))

    val body : List[Byte] = serializeLongString (queryString) :::
      serializeShort(0x001) ::: // consistency
      List[Byte](0x00) ::: // query flags
      List[Byte]()

    stream.write(body.toArray)
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

  def serializeLongString(string: String): List[Byte] = {
    serializeInt(string.length) :::
      serializeString(string)
  }

  def serializeInt(int: Int): List[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putInt(int)
    frameBuilder.result().toList
  }

  def serializeString(string: String): List[Byte] = {
    string.getBytes.toList
  }

  def serializeShort(short : Short) : List[Byte] = {
    val frameBuilder = ByteString.newBuilder
    frameBuilder.putShort(short)
    frameBuilder.result().toList
  }
  
  def sendQuery(query : Array[Byte])(implicit stream : OutputStream) = {
    val queryParamsWithConsistencyOfANY = Array[Byte](0x00, 0x00)

    stream.write(Array[Byte](0x2, 0x0, 0x0, OpCodes.Query))
    val bodyLengthAsByte = (query.length + 2).toByte
    stream.write(serializeInt(bodyLengthAsByte).toArray)

    stream.write(query)
    stream.write(queryParamsWithConsistencyOfANY)

  }

}

object ConnectionToServerStub {
  val ServerHost = "localhost"
  val ServerPort = 8042

  def apply() = new Socket(ServerHost, ServerPort)
}

object ServerStubAsThread {
  def apply() = new Thread(new Runnable {
    def run() {
      ServerStubRunner.run()
    }
  })
}