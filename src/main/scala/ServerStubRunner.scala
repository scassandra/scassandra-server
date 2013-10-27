import java.io.{DataOutputStream, OutputStreamWriter, PrintWriter}
import java.net.{ServerSocket, Socket}

object ResponseHeader {
  val VersionByte = 0x82
  val FlagsNoCompressionByte = 0x00
  val DefaultStreamId = 0x00
  val ZeroLength = Array(0x00, 0x00, 0x00, 0x00).map(_.toByte)

  object OpCodes {
    val Ready = 0x02
  }

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
      out.write(ResponseHeader.VersionByte)
      out.write(ResponseHeader.FlagsNoCompressionByte)
      out.write(ResponseHeader.DefaultStreamId)
      out.write(ResponseHeader.OpCodes.Ready)
      out.write(ResponseHeader.ZeroLength)
      out.flush()

      // TODO: call close on the socket, or the output stream, or both?
      clientSocket.close()
    }
  }
}
