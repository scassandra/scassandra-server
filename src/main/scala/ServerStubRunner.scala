import java.io.{DataInputStream, DataOutputStream}
import com.typesafe.scalalogging.slf4j.Logging
import java.net.{ServerSocket, Socket}

object HeaderConsts {
  val Length = 4
}

object ServerStubRunner extends Logging {

  var portNumber = 8042

  def sendMessage(socket: Socket, bytes: List[Int]) = {
    val out = new DataOutputStream(socket.getOutputStream)
    for (byte <- bytes) {
      out.write(byte)
    }
    out.flush()
  }

  def main(args: Array[String]) {
    if (args.length > 0) {
      val port = args(0)
      logger.info(s"Overriding port to ${port}")
      portNumber = port.toInt
    }
    run()
  }


  def run() = {

    val serverSocket = new ServerSocket(portNumber)

    logger.info(s"Server started on port ${portNumber}")

    while (true) {

      val clientSocket: Socket = serverSocket.accept()

      // TODO: "java.net.SocketException: Connection reset" could happen if the socket is closed from the other side while this is still trying to write to it.

      val in = new DataInputStream(clientSocket.getInputStream)

      val header = readRawBytes(in, HeaderConsts.Length)

      val messageLength = readInteger(in)

      // we ignore the rest of the message for now
      readRawBytes(in, messageLength)

      header(3) match {
        case OpCodes.Startup => {
          logger.info("Sending ready message")
          sendMessage(clientSocket, Ready.serialize())
        }
        case OpCodes.Query => {
          logger.info("Sending result")
          // TODO: Parse the query and see if it is a use statement
          sendMessage(clientSocket, VoidResult.serialize())
        }
        case opCode @ _ => {
          logger.info(s"Received unknown opcode ${opCode}")
        }
      }

      // TODO: call close on the socket, or the output stream, or both?
      clientSocket.close()
    }
  }

  def readRawBytes(in: DataInputStream, numberOfBytes: Int): Seq[Byte] = {
    for {
      i <- 0 until numberOfBytes
    } yield in.read().toByte
  }

  //TODO: Only works for values up to ~32k as furst three bytes are ignored
  def readInteger(in: DataInputStream): Int = {
    for (i <- 0 until 3) {
      in.read()
    }
    in.read()
  }
}
