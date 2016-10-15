package org.scassandra.codec.datatype

import org.scassandra.codec.datatype.DataType.Custom
import scodec.Attempt.Failure
import scodec.Err.General
import scodec.bits.ByteVector

class CustomSpec extends DataTypeSpec {

  val custom = Custom("MyType")
  val codec = custom.codec

  "codec" must "encode Array[Byte] and decode into ByteVector" in {
    val bytes = Array(0x86.toByte, 0x75.toByte, 0x30.toByte, 0x9.toByte)

    encodeAndDecode(codec, bytes, ByteVector.fromValidHex("0x86753009"))
  }

  it must "encode ByteVector and decode into ByteVector" in {
    val bytes = ByteVector(0x86, 0x75, 0x30, 0x9)

    encodeAndDecode(codec, bytes)
  }

  it must "encode BitVector and decode into ByteVector" in {
    val bytes = ByteVector(0x86, 0x75, 0x30, 0x9)

    encodeAndDecode(codec, bytes.bits, bytes)
  }

  it must "encode string as hexString into ByteVector" in {
    codec.encode("1235").require.bytes shouldEqual ByteVector(18, 53)
    codec.encode("0x01").require.bytes shouldEqual ByteVector(1)
  }

  it must "fail to encode a string that isn't a hexString" in {
    codec.encode("hello") should matchPattern { case Failure(_) => }
  }

  it must "fail to encode something that isn't a byte representation" in {
    codec.encode(BigDecimal(123.67)) should matchPattern { case Failure(General("Unsure how to encode 123.67", Nil)) => }
    codec.encode(true) should matchPattern { case Failure(General("Unsure how to encode true", Nil)) => }
    codec.encode(false) should matchPattern { case Failure(General("Unsure how to encode false", Nil)) => }
    codec.encode(List()) should matchPattern { case Failure(General("Unsure how to encode List()", Nil)) => }
    codec.encode(Map()) should matchPattern { case Failure(General("Unsure how to encode Map()", Nil)) => }
  }
}
