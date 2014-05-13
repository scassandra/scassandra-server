package uk.co.scassandra.cqlmessages.request

import org.scalatest.{Matchers, FunSuite}
import uk.co.scassandra.cqlmessages._
import akka.util.ByteString

class ExecuteRequestTest extends FunSuite with Matchers {

  import CqlProtocolHelper._

  test("Seralisation of a execute") {
    val stream : Byte = 0x01
    val protocolVersion : Byte = 0x1
    val consistency = TWO
    val id : Byte = 5
    val executeRequest = new ExecuteRequest(protocolVersion, stream, id, consistency)
    val serialisation = executeRequest.serialize().iterator

    serialisation.getByte should equal(protocolVersion)
    serialisation.getByte // ignore the flags
    serialisation.getByte should equal(stream)
    serialisation.getByte should equal(OpCodes.Execute)

    serialisation.drop(4) // length

    CqlProtocolHelper.readShortBytes(serialisation) should equal(Array[Byte](0,0,0,id))
    serialisation.getShort should equal(consistency.code)
    serialisation.getByte should equal(0) // flags

    val numberOfOptions = serialisation.getShort
    numberOfOptions should equal(0)

    serialisation.isEmpty should equal(true)
  }

  test("Deserialise execute with numeric variable types") {
    val stream: Byte = 5
    val v2MessageFromCassandra = ByteString(
      0, 4, // length of the prepared statement id
      0, 0, 0, 1, // prepared statement id
      0, 1, // consistency
      5, // flags
      0, 7, // number of variables
      0, 0, 0, 8,    0, 0, 0, 0, 0, 0, 4, -46, //   val bigInt : java.lang.Long = 1234
      0, 0, 0, 8,    0, 0, 0, 0, 0, 0, 9, 41,    //    val counter : java.lang.Long = 2345
      0, 0, 0, 5,    0, 0, 0, 0, 1,              //   val decimal : java.math.BigDecimal = new java.math.BigDecimal("1")
      0, 0, 0, 8,    63, -8, 0, 0, 0, 0, 0, 0,   //   val double : java.lang.Double = 1.5
      0, 0, 0, 4,    64, 32, 0, 0,               //   val float : java.lang.Float = 2.5f
      0, 0, 0, 4,    0, 0, 13, -128,             //   val int : java.lang.Integer = 3456
      0, 0, 0, 1,   123,                         //   val varint : java.math.BigInteger = new java.math.BigInteger("123")

      0, 0, 19, -120) // serial consistency?? not sure

    val types = List[ColumnType[_]](CqlBigint, CqlCounter, CqlDecimal, CqlDouble, CqlFloat, CqlInt, CqlVarint)

    val response = ExecuteRequest.versionTwoWithTypes(stream, v2MessageFromCassandra, types)

    response.consistency should equal(ONE)
    response.id should equal(1)
    response.flags should equal(5)
    response.stream should equal(stream)
    response.numberOfVariables should equal(7)
    response.variables.size should equal(7)
    response.variables should equal(
      List(1234l, 2345l, BigDecimal("1"), 1.5d, 2.5f, 3456, BigInt("123"))
    )
  }

  test("Deserailise without parsing the variable types") {
    val stream: Byte = 5
    val v2MessageFromCassandra = ByteString(
      0, 4, // length of the prepared statement id
      0, 0, 0, 1, // prepared statement id
      0, 1, // consistency
      5, // flags
      0, 7, // number of variables
      0, 0, 0, 8,    0, 0, 0, 0, 0, 0, 4, -46, //   val bigInt : java.lang.Long = 1234
      0, 0, 0, 8,    0, 0, 0, 0, 0, 0, 9, 41,    //    val counter : java.lang.Long = 2345
      0, 0, 0, 5,    0, 0, 0, 0, 1,              //   val decimal : java.math.BigDecimal = new java.math.BigDecimal("1")
      0, 0, 0, 8,    63, -8, 0, 0, 0, 0, 0, 0,   //   val double : java.lang.Double = 1.5
      0, 0, 0, 4,    64, 32, 0, 0,               //   val float : java.lang.Float = 2.5f
      0, 0, 0, 4,    0, 0, 13, -128,             //   val int : java.lang.Integer = 3456
      0, 0, 0, 1,   123,                         //   val varint : java.math.BigInteger = new java.math.BigInteger("123")

      0, 0, 19, -120) // serial consistency?? not sure

    val response = ExecuteRequest.versionTwoWithoutTypes(stream, v2MessageFromCassandra)

    response.consistency should equal(ONE)
    response.id should equal(1)
    response.flags should equal(5)
    response.stream should equal(stream)
    response.variables.size should equal(0)
    response.numberOfVariables should equal(7)
  }
}
