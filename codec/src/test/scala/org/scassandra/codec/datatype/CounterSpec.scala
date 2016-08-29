package org.scassandra.codec.datatype

import org.scassandra.codec.datatype.DataType.Counter
import scodec.Attempt.Failure
import scodec.bits.ByteVector

class CounterSpec extends DataTypeSpec {

  val codec = Counter.codec

  "codec" must "encode any Number as long" in {
    codec.encode(BigDecimal("123000000000")).require.bytes shouldEqual ByteVector(0, 0, 0, 28, -93, 95, 14, 0)
    codec.encode("123").require.bytes shouldEqual ByteVector(0, 0, 0, 0, 0, 0, 0, 123)
  }

  it must "fail to encode a string representation that doesn't map to long" in {
    codec.encode("hello") should matchPattern { case Failure(_) => }
  }

  it must "fail to encode type that is not a String or Number" in {
    codec.encode(true) should matchPattern { case Failure(_) => }
    codec.encode(false) should matchPattern { case Failure(_) => }
    codec.encode(List()) should matchPattern { case Failure(_) => }
    codec.encode(Map()) should matchPattern { case Failure(_) => }
  }
}
