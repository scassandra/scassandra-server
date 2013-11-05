import java.io._
import java.net.{ServerSocket, Socket}

object OpCodes {
  val Ready : Byte = 0x02
  val Startup : Byte = 0x01
  val Options : Byte = 0x05
}

object ResponseHeader {
  val VersionByte = 0x82
  val FlagsNoCompressionByte = 0x00
  val DefaultStreamId = 0x00
  val ZeroLength = Array(0x00, 0x00, 0x00, 0x00).map(_.toByte)
}

object Header {
  val Length = 4
}

object ServerStubRunner {

  val PortNumber = 9042

  def main(args: Array[String]) {
    run()
  }


  def run() = {

    val serverSocket = new ServerSocket(PortNumber)

    while (true) {

      val clientSocket: Socket = serverSocket.accept()

      // TODO: "java.net.SocketException: Connection reset" could happen if the socket is closed from the other side while this is still trying to write to it.
      val out = new DataOutputStream(clientSocket.getOutputStream)
      val in = new DataInputStream(clientSocket.getInputStream)

      val header = readRawBytes(in, Header.Length)

      val messageLength = readInteger(in)

      // we ignore the rest of the message for now
      readRawBytes(in, messageLength)

      if (header(3) == OpCodes.Ready) {
        out.write(ResponseHeader.VersionByte)
        out.write(ResponseHeader.FlagsNoCompressionByte)
        out.write(ResponseHeader.DefaultStreamId)
        out.write(OpCodes.Ready)
        out.write(ResponseHeader.ZeroLength)
        out.flush()
      }

      // TODO: call close on the socket, or the output stream, or both?
      clientSocket.close()
    }
  }

  def readRawBytes(in : DataInputStream, numberOfBytes : Int) : Seq[Byte] = {
    for {
      i <- 0 until numberOfBytes
    } yield in.read().toByte
  }

  //TODO: Only works for values up to ~32k as furst three bytes are ignored
  def readInteger(in : DataInputStream) : Int = {
    for (i <- 0 until 3) { in.read() }
    in.read()
  }
}
