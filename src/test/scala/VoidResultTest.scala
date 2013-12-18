import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.matchers.ShouldMatchers

class VoidResultTest extends FunSuite with ShouldMatchers {

  test("Serialisation of a void result") {
    val voidResult = VoidResult
    val bytes = voidResult.serialize()

    bytes should equal(List(
      0x82, // protocol version
      0x00, // flags
      0x00, // stream
      0x08, // message type - 8 (Result)
      0x0, 0x0, 0x0, 0x4, // 4 byte integer - length (number of bytes)
      0x0,0x0, 0x0, 0x1 // 4 byte integer for the type of result, 1 is a void result
    ))
  }
}
