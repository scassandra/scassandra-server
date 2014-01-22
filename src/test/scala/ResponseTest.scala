import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers

class ResponseTest extends FunSuite with ShouldMatchers {

  test("Serialisation of a void result") {
    val stream : Byte = 0x01
    val voidResult = VoidResult(stream)
    val bytes = voidResult.serialize().toList

    bytes should equal(List[Byte](
      (0x82 & 0xFF).toByte, // protocol version
      0x00, // flags
      stream, // stream
      0x08, // message type - 8 (Result)
      0x0, 0x0, 0x0, 0x4, // 4 byte integer - length (number of bytes)
      0x0,0x0, 0x0, 0x1 // 4 byte integer for the type of result, 1 is a void result
    ))
  }
  
  test("Serialisation of a ready response") {
    val readyMessage = Ready()
    val bytes = readyMessage.serialize().toList

    bytes should equal(List[Byte](
      (0x82 & 0xFF).toByte, // protocol version
      0x00, // flags
      0x00, // stream
      0x02, // message type - 2 (Ready)
      0x0, 0x0, 0x0, 0x0 // 4 byte integer - length (number of bytes)
    ))
  }

  test("Serialisation of a error response client protocol error") {
    val errorCode : Byte = 0xA
    val errorText = "Any old error message"
    val stream : Byte = 0x04
    val errorMessage = new Error(errorCode, errorText, stream)
    val bytes : List[Byte] = errorMessage.serialize().toList

    bytes should equal(List[Byte](
      (0x82 & 0xFF).toByte, // protocol version
      0x00, // flags
      stream, // stream
      0x00, // message type - 2 (Error)
      0x0, 0x0, 0x0, (errorText.length + 6).toByte, // 4 byte integer - length (number of bytes)
      0x0, 0x0, 0x0, errorCode,
      0x00, errorText.length.toByte) ::: // length of the errorText
      errorText.getBytes.toList
    )
  }

  test("QueryBeforeReadyMessage is Error message with code 0xA") {
    val stream : Byte = 0x03
    val errorQueryBeforeReadyMessage = QueryBeforeReadyMessage(stream)

    errorQueryBeforeReadyMessage.header.opCode should equal(OpCodes.Error)
    errorQueryBeforeReadyMessage.errorCode should equal(0xA)
    errorQueryBeforeReadyMessage.errorMessage should equal("Query sent before StartUp message")
    errorQueryBeforeReadyMessage.header.streamId should equal(stream)
  }

  test("Serialisation of a setkeyspace result response") {
    val keyspaceName = "people"
    val stream : Byte = 0x02
    val setKeyspaceMessage = SetKeyspace(keyspaceName, stream)
    val bytes = setKeyspaceMessage.serialize()

    bytes should equal(List[Byte](
      (0x82 & 0xFF).toByte, // protocol version
      0x00, // flags
      stream, // stream
      0x08, // message type - 8 (Result)
      0x0, 0x0, 0x0, (keyspaceName.length + 6).toByte, // 4 byte integer - length (number of bytes)
      0x0, 0x0, 0x0, 0x3, // type of result - set_keyspace
      0x0, keyspaceName.size.toByte) :::
      keyspaceName.getBytes().toList)
  }

}
