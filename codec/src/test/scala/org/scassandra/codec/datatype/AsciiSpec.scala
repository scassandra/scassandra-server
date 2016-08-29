package org.scassandra.codec.datatype

import org.scassandra.codec.datatype.DataType.Ascii
import scodec.Attempt.Failure
import scodec.Err.General
import scodec.bits.ByteVector

class AsciiSpec extends DataTypeSpec {

  val codec = Ascii.codec

  "codec" must "encode a string as UTF-8" in {
    codec.encode("hello").require.bytes shouldEqual ByteVector(104, 101, 108, 108, 111)
  }

  it must "encode empty string to empty bytes" in {
    codec.encode("").require.bytes shouldEqual ByteVector.empty
  }

  it must "encode any non-String as its toString representation" in {
    codec.encode(BigDecimal("123.67")).require.bytes shouldEqual ByteVector(49, 50, 51, 46, 54, 55)
    codec.encode(true).require.bytes shouldEqual ByteVector(116, 114, 117, 101)
  }

  it must "fail when encoding Iterable" in {
    codec.encode(Nil) should matchPattern { case Failure(General("Unsure how to encode List()", Nil)) => }
    codec.encode(Set(1)) should matchPattern { case Failure(General("Unsure how to encode Set(1)", Nil)) => }
  }

  it must "fail when encoding Map" in {
    codec.encode(Map()) should matchPattern { case Failure(General("Unsure how to encode Map()", Nil)) => }
  }

  it must "fail to encode non-ascii text" in {
    codec.encode("hello ⌦ utf➑") should matchPattern { case Failure(General("US-ASCII cannot encode character '⌦'", Nil)) => }
  }

  it must "encode and decode back to input as string" in {
    encodeAndDecode(codec, "hello")
    encodeAndDecode(codec, "")
    encodeAndDecode(codec, 123, "123")
    encodeAndDecode(codec, BigDecimal("123.67"), "123.67")
  }
}
