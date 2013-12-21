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
    println("CLosing socket")
    connectionToServerStub.close()
  }

  test("return nothing until a startup message is received") {
    val bytes = availableBytes(200)

    bytes should equal(0)

    sendStartupMessage()

    //TODO: Verify contents?
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
    sendStartupMessage()

    val in = new DataInputStream(connectionToServerStub.getInputStream)

    // consume first two bytes
    consumeBytes(in, 2)

    // read third byte
    val responseHeaderStream: Int = in.read()

    // TODO: in future versions, the stream ID should reflect the value set in the request header
    val noStreamId = 0x00
    responseHeaderStream should equal(noStreamId)
  }

  test("return a READY opcode byte in the response header on STARTUP request") {
    sendStartupMessage()

    val in = new DataInputStream(connectionToServerStub.getInputStream)

    // consume first three bytes
    consumeBytes(in, 3)

    // read fourth byte
    val responseHeaderOpCode: Int = in.read()

    val opCodeReady = 0x02
    responseHeaderOpCode should equal(opCodeReady)
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

  def readReadyMessage() = {
    val stream: DataInputStream = new DataInputStream(connectionToServerStub.getInputStream)
    consumeBytes(stream, 8)
  }

  test("return RESULT OpCode on Query") {

    sendStartupMessage()
    readReadyMessage()
    //
    //
    // A select query has: <header><body>
    // where <header> is the usual and
    // body = <query><query_parameters>
    // where <query> = well, the query...
    // and  <query_parameters> = <consistency><other_things>
    //
    // For simplicity, <other_things> will not be set in this test case
    //

    val stream: OutputStream = connectionToServerStub.getOutputStream

    val queryAsBytes = "select * from people".toCharArray.map(_.toByte)

    // Header - Part 1
    // TODO - Confirm what the first three bytes should be
    stream.write(Array[Byte](0x02, 0x00, 0x00, OpCodes.Query))

    // Header - Part 2
    // TODO - how to convert an Integer into an array of Bytes? Luckily this length fits into one byte
    // body length = query length + query parameters length.
    val bodyLengthAsByte = (queryAsBytes.length + 2).toByte
    stream.write(Array[Byte](0x00, 0x00, 0x00, bodyLengthAsByte))


    // Body - Part 1 : <query>
    stream.write(queryAsBytes)

    // Body - Part 2 : <query_parameters>
    // In this case, we only set <consistency> = short to 0x0000 -> ANY
    stream.write(Array[Byte](0x00, 0x00))

    val in = new DataInputStream(connectionToServerStub.getInputStream)

    // consume first three bytes
    consumeBytes(in, 3)

    // read fourth byte
    val responseHeaderOpCode: Int = in.read()

    val opCodeResult = 0x08
    responseHeaderOpCode should equal(opCodeResult)

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
    Thread.sleep(1000)
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