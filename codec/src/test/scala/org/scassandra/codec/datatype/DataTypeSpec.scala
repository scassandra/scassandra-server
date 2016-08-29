package org.scassandra.codec.datatype

import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import org.scassandra.codec.ProtocolVersion
import scodec.Codec

class DataTypeSpec extends FlatSpec with Checkers with Matchers {

  implicit val protocolVersion = ProtocolVersion.latest

  def encodeAndDecode[T](codec: Codec[Any], data: T): Unit = {
    encodeAndDecode(codec, data, data)
  }

  def encodeAndDecode[T](codec: Codec[Any], data: T, expected: Any): Unit = {
    val encoded = codec.encode(data).require
    val decoded = codec.decodeValue(encoded).require

    val expectedResult = expected
    decoded shouldEqual expectedResult
  }

}
