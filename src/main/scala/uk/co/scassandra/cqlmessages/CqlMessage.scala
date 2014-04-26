package uk.co.scassandra.cqlmessages

import akka.util.ByteString

abstract class CqlMessage(val header: Header) {
  def serialize() : ByteString
}
