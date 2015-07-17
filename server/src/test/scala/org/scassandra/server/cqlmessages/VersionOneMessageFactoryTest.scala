package org.scassandra.server.cqlmessages

import akka.util.ByteString
import org.scalatest.{Matchers, FunSuite}
import org.scassandra.server.actors.QueryFlags
import org.scassandra.server.cqlmessages.types.CqlText

class VersionOneMessageFactoryTest extends FunSuite with Matchers {

  implicit val protocolVersion = VersionOne
  val underTest = VersionOneMessageFactory

  test("Parsing QueryRequest") {
    val query: String = "select * from blah where name = ?"
    val message: Array[Byte] =
                CqlProtocolHelper.serializeLongString(query) ++
                CqlProtocolHelper.serializeShort(TWO.code) ++
                Array[Byte](QueryFlags.Values) ++
                CqlProtocolHelper.serializeShort(1) ++ // number of params
                CqlText.writeValueWithLength("Hello")

    val queryRequest = underTest.parseQueryRequest(1, ByteString(message), List(CqlText))

    queryRequest.query should equal(query)
    queryRequest.consistency should equal(TWO.code)
    queryRequest.flags should equal(QueryFlags.Values)
    queryRequest.parameters should equal(List(Some("Hello")))

  }
}
