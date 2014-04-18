package uk.co.scassandra.cqlmessages

import org.scalatest.{Matchers, FunSuite}
import org.scassandra.cqlmessages.{VersionOne, VersionTwo, ProtocolVersion}

class ProtocolVersionsTest extends FunSuite with Matchers {
  test("Mapping client protocol version to server - version 1") {
    ProtocolVersion.protocol(ProtocolVersion.ClientProtocolVersionOne) should equal(VersionOne)
  }

  test("Mapping client protocol version to server - version 2") {
    ProtocolVersion.protocol(ProtocolVersion.ClientProtocolVersionTwo) should equal(VersionTwo)
  }
}
