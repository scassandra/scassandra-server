package org.scassandra.cqlmessages.response

import org.scassandra.cqlmessages.CqlProtocolHelper
import org.scassandra.cqlmessages.types.{CqlMap, CqlList, CqlSet, ColumnType}
import akka.util.ByteStringBuilder

object ResultHelper {

  import CqlProtocolHelper._

  def serialiseTypeInfomration(name: String, columnType: ColumnType[_], iterator: ByteStringBuilder) = {
    iterator.putBytes(CqlProtocolHelper.serializeString(name).toArray)
    iterator.putShort(columnType.code)
    columnType match {
      case CqlSet(setType) => iterator.putShort(setType.code)
      case CqlList(listType) => iterator.putShort(listType.code)
      case CqlMap(keyType, valueType) => {
        iterator.putShort(keyType.code)
        iterator.putShort(valueType.code)
      }
      case _ => //no-op
    }
  }
}
