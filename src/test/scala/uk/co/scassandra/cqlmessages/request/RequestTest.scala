package uk.co.scassandra.cqlmessages.request

import org.scalatest._
import uk.co.scassandra.cqlmessages._

class RequestTest extends FunSuite with Matchers {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  test("Serialization of StartupRequest") {
    val body = StartupRequest
    body.serialize() should equal(
      Seq(
        ProtocolVersion.ClientProtocolVersionTwo, 0, 0, OpCodes.Startup,// header
        0, 0, 0, 22, // length
        0x00, 0x01, // length of map
        0x00, 0x0b, // length of key
        0x043, 0x051, 0x4c, 0x5f, 0x56, 0x45, 0x52, 0x53, 0x49, 0x4f, 0x4e, // "CQL_VERSION"
        0x0, 0x5, // length of value
        0x33, 0x2e, 0x30, 0x2e, 0x30)) // "3.0.0"
  }

  test("Serialization of Query") {
    val queryString = "use people"
    val stream : Byte = 0x01
    val queryRequest = new QueryRequest(stream, queryString)
    val serialzation = queryRequest.serialize()
    serialzation should equal(
      Seq(
        ProtocolVersion.ClientProtocolVersionTwo, 0, stream, OpCodes.Query,
        0, 0, 0, 17, 
        0x00, 0x00, 0x00, 0x0a, // length of query
        0x75, 0x73, 0x65, 0x20, 0x70, 0x65, 0x6f, 0x70, 0x6c, 0x65, // query as ascii hex
        0x00, 0x01, // consistency of 1
        0x00 // flags - none set
      )
    )
  }

  test("Serialization of Prepare") {
    val queryString = "use people"
    val stream : Byte = 0x01
    val protocolVersion = VersionOne.clientCode
    val prepareRequest = new PrepareRequest(protocolVersion, stream, queryString)
    val serialisation = prepareRequest.serialize()
    serialisation should equal(
      Seq(
        ProtocolVersion.ClientProtocolVersionOne, 0, stream, OpCodes.Prepare,
        0, 0, 0, 14,
        0x00, 0x00, 0x00, 0x0a, // length of query
        0x75, 0x73, 0x65, 0x20, 0x70, 0x65, 0x6f, 0x70, 0x6c, 0x65 // query as ascii hex
      )
    )
  }

}
