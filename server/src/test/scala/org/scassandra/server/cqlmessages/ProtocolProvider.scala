package org.scassandra.server.cqlmessages

trait ProtocolProvider {
  implicit val protocolVersion = VersionTwo
}
