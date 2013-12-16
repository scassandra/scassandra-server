import java.io._
import java.net.Socket
import java.net.ConnectException
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import scala.collection.immutable.IndexedSeq

object LocalSocket {
  val ServerHost = "localhost"
  val ServerPort = 8042

  def apply() = new Socket(ServerHost, ServerPort)
}

// TODO: the server is not multi-threaded for now. Decide what concurrency pattern to use: Actors, Futures, Executors, etc
object ServerStubAsThread {

  def apply() = new Thread(new Runnable {
    def run() {
      ServerStubRunner.run()
    }
  })
}

class ServerStubRunnerTest extends FunSuite with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {
  var serverThread: Thread = null
  var socket: Socket = null

  before {
    socket = LocalSocket()
  }

  after {
    consumeAllBytes(socket)
  }

  override def beforeAll {

    // First ensure nothing else is running on port 9042
    var somethingAlreadyRunning = true

    try {
      // TODO: use "new" instead? Note: does not throw exception when called without brackets...
      LocalSocket()
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


  def consumeAllBytes(socket: Socket) {
    if (!socket.isClosed) {
      val stream = new DataInputStream(socket.getInputStream)

      var byte = stream.read()
      while (byte != -1) {
        byte = stream.read()
      }
    }

  }

  def availableBytes(timeToWaitMillis: Long): Int = {
    // TODO: Make this check every N millis rather than wait the full amount first?
    Thread.sleep(timeToWaitMillis)
    val stream = new DataInputStream(socket.getInputStream)
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
    Thread.sleep(100)
  }

  def stopServerStub() = {
    serverThread.interrupt()
  }

  def sendStartupMessage() = {
    val stream: OutputStream = socket.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, 0x00, OpCodes.Startup))
    stream.write(Array[Byte](0x00, 0x00, 0x00, 0x16))
    val fakeBody: IndexedSeq[Byte] = for (i <- 0 until 22) yield 0x00.toByte
    stream.write(fakeBody.toArray)
  }

  def sendOptionsMessage {
    val stream: OutputStream = socket.getOutputStream
    stream.write(Array[Byte](0x02, 0x00, 0x00, OpCodes.Options))
    sendFakeLengthAndBody(stream)
  }

  def sendFakeLengthAndBody(stream: OutputStream) {
    stream.write(Array[Byte](0x00, 0x00, 0x00, 0x16))
    val fakeBody: IndexedSeq[Byte] = for (i <- 0 until 22) yield 0x00.toByte
    stream.write(fakeBody.toArray)
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

    val in = new DataInputStream(socket.getInputStream)

    // read first byte
    val responseHeaderVersion: Int = in.read()

    val responseHeaderVersionTwo = 0x82
    responseHeaderVersion should equal(responseHeaderVersionTwo)
  }

  test("return a flags byte in the response header with all bits set to 0 on STARTUP request") {
    sendStartupMessage()

    val in = new DataInputStream(socket.getInputStream)

    // consume first byte
    consumeBytes(in, 1)

    // read second byte
    val responseHeaderFlags: Int = in.read()

    val noCompressionFlags = 0x00
    responseHeaderFlags should equal(noCompressionFlags)
  }

  test("return a stream byte in the response header") {

    sendStartupMessage()

    val in = new DataInputStream(socket.getInputStream)

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

    val in = new DataInputStream(socket.getInputStream)

    // consume first three bytes
    consumeBytes(in, 3)

    // read fourth byte
    val responseHeaderOpCode: Int = in.read()

    val opCodeReady = 0x02
    responseHeaderOpCode should equal(opCodeReady)
  }

  test("return length field with all 4 bytes set to 0 on STARTUP request") {

    sendStartupMessage()

    val in = new DataInputStream(socket.getInputStream)

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

    val stream: OutputStream = socket.getOutputStream

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

    val in = new DataInputStream(socket.getInputStream)

    // consume first three bytes
    consumeBytes(in, 3)

    // read fourth byte
    val responseHeaderOpCode: Int = in.read()

    val opCodeResult = 0x08
    responseHeaderOpCode should equal(opCodeResult)

  }
}