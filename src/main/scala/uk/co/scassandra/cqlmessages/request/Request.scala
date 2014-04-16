package org.scassandra.cqlmessages.request

import akka.util.ByteString
import org.scassandra.cqlmessages._

// Current has a fixed stream ID as only sent on startup
case object StartupHeader extends Header(HeaderConsts.ClientProtocolVersion, opCode = OpCodes.Startup, streamId = 0)

case class QueryHeader(stream: Byte) extends Header(HeaderConsts.ClientProtocolVersion, opCode = OpCodes.Query, streamId = stream)

abstract class Request(header: Header) extends CqlMessage(header)

object StartupRequest extends Request(StartupHeader) {

  val options = Map("CQL_VERSION" -> "3.0.0")

  override def serialize() = {
    val header = StartupHeader.serialize()

    val body = CqlProtocolHelper.serializeShort(options.size.toShort) ++
      CqlProtocolHelper.serializeString(options.head._1) ++
      CqlProtocolHelper.serializeString(options.head._2)

    ByteString((header ++ CqlProtocolHelper.serializeInt(body.size) ++ body))
  }
}

case class QueryRequest(stream: Byte, query: String, val consistency : Short = 0x0001, val flags : Byte = 0x00) extends Request(QueryHeader(stream)) {
  override def serialize() = {
    val header = QueryHeader(stream).serialize()
    header.toList
    val body =  CqlProtocolHelper.serializeLongString(query) ++
      CqlProtocolHelper.serializeShort(consistency) ++
      CqlProtocolHelper.serializeByte(flags)

    header ++ CqlProtocolHelper.serializeInt(body.size) ++ body
    ByteString((header ++ CqlProtocolHelper.serializeInt(body.size) ++ body))
  }
}




