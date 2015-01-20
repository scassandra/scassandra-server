package org.scassandra.server.priming

import akka.util.ByteString

object QueryHandlerMessages {
  case class Query(queryBody: ByteString, stream: Byte)
}