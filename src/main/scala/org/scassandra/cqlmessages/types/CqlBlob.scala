package org.scassandra.cqlmessages.types

import akka.util.{ByteString, ByteIterator}
import org.scassandra.cqlmessages.CqlProtocolHelper

case object CqlBlob extends ColumnType[Option[Array[Byte]]](0x0003, "blob") {

  import CqlProtocolHelper._

   override def readValue(byteIterator: ByteIterator) = {
     CqlProtocolHelper.readBlobValue(byteIterator)
   }

   override def writeValue(value: Any) = {
     val bs = ByteString.newBuilder
     val array = hex2Bytes(value.toString)
     bs.putInt(array.length)
     bs.putBytes(array)
     bs.result().toArray
   }

   private def hex2Bytes(hex: String): Array[Byte] = {
     try {
       (for {i <- 0 to hex.length - 1 by 2 if i > 0 || !hex.startsWith("0x")}
       yield hex.substring(i, i + 2))
         .map(hexValue => Integer.parseInt(hexValue, 16).toByte).toArray
     }
     catch {
       case s : Exception => throw new IllegalArgumentException(s"Not valid hex $hex")
     }
   }
 }
