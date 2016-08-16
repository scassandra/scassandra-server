package org.scassandra.codec

import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import org.scassandra.codec.datatype.DataType
import scodec.Attempt.Failure

class TupleSpec extends FlatSpec with Checkers with Matchers {

  implicit val protocolVersion = ProtocolVersion.latest

  "TupleCodec" must "properly encode and decode a tuple<int,text> from a List" in {
    val tupleType = DataType.Tuple(DataType.Int, DataType.Text)

    val data = List(55, "Hello World")

    val encoded = tupleType.codec.encode(data).require
    val decoded = tupleType.codec.decodeValue(encoded).require

    decoded shouldEqual data
  }

  it must "properly handle null values in a List" in {
    val tupleType = DataType.Tuple(DataType.Int, DataType.Int, DataType.Text)

    val data = List(55, null, "Hello World")

    val encoded = tupleType.codec.encode(data).require
    val decoded = tupleType.codec.decodeValue(encoded).require

    decoded shouldEqual data
  }

  it must "fail to encode when List length does not match tuple length" in {
    val tupleType = DataType.Tuple(DataType.Int, DataType.Text, DataType.Int)

    val data = List(55, "Hello")

    tupleType.codec.encode(data) should matchPattern { case Failure(_) => }
  }

  it must "be able to encode scala Tuples" in {
    val tupleType = DataType.Tuple(DataType.Int, DataType.Int)

    val data = (5, 3)

    val encoded = tupleType.codec.encode(data).require
    val decoded = tupleType.codec.decodeValue(encoded).require

    decoded shouldEqual data.productIterator.toList
  }

}
