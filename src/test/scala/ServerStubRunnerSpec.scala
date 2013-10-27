import java.io._
import java.net.Socket
import java.net.ConnectException
import org.specs2.mutable._
import org.specs2.specification.{AfterExample, BeforeExample}

// TODO: name this LocalClientSocketBuilder instead?
object LocalSocket {
  val ServerHost = "localhost"
  val ServerPort = 9042

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

class ServerStubRunnerSpec extends Specification {
  var serverThread: Thread = null
  var socket: Socket = null

  implicit val context = new BeforeAfter {

    def before = {
      socket = LocalSocket()
    }

    def after = consumeAllBytes(socket)
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


  "run()" should {
    // before all
    step {
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
        failure("There must not be any server already running")
      }

      // Then start the server
      startServerStub()
    }

    "return a version byte in the response header" in {

      val in = new DataInputStream(socket.getInputStream)

      // read first byte
      val responseHeaderVersion: Int = in.read()

      val responseHeaderVersionTwo = 0x82
      responseHeaderVersion must equalTo(responseHeaderVersionTwo)
    }

    "return a flags byte in the response header with all bits set to 0 on STARTUP request" in {

      // TODO: send actual STARTUP request header and body

      val in = new DataInputStream(socket.getInputStream)

      // consume first byte
      consumeBytes(in, 1)

      // read second byte
      val responseHeaderFlags: Int = in.read()

      val noCompressionFlags = 0x00
      responseHeaderFlags must equalTo(noCompressionFlags)
    }

    "return a stream byte in the response header" in {

      // TODO: send actual STARTUP request header and body

      val in = new DataInputStream(socket.getInputStream)

      // consume first two bytes
      consumeBytes(in, 2)

      // read third byte
      val responseHeaderStream: Int = in.read()

      // TODO: in future versions, the stream ID should reflect the value set in the request header
      val noStreamId = 0x00
      responseHeaderStream must equalTo(noStreamId)
    }

    "return a READY opcode byte in the response header on STARTUP request" in {

      // TODO: send actual STARTUP request header and body

      val in = new DataInputStream(socket.getInputStream)

      // consume first three bytes
      consumeBytes(in, 3)

      // read fourth byte
      val responseHeaderOpCode: Int = in.read()

      val opCodeReady = 0x02
      responseHeaderOpCode must equalTo(opCodeReady)
    }

    "return length field with all 4 bytes set to 0 on STARTUP request" in {

      // TODO: send actual STARTUP request header and body

      val in = new DataInputStream(socket.getInputStream)

      // consume first four bytes
      consumeBytes(in, 4)

      // read next four bytes
      var length = 0
      for (a <- 1 to 4) {
        length += in.read()
      }

      length must equalTo(0)
    }

    //    after
    step {
      stopServerStub()
    }
  }
}