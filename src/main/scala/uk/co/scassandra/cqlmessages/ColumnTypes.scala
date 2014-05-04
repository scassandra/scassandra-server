package uk.co.scassandra.cqlmessages

abstract class ColumnType(val code : Short, val stringRep: String)

case object CqlAscii extends ColumnType(0x0001, "ascii")
case object CqlBigint extends ColumnType(0x0002, "bigint")
case object CqlBlob extends ColumnType(0x0003, "blob")
case object CqlBoolean extends ColumnType(0x0004, "boolean")
case object CqlCounter extends ColumnType(0x0005, "counter")
case object CqlDecimal extends ColumnType(0x0006, "decimal")
case object CqlDouble extends ColumnType(0x0007, "double")
case object CqlFloat extends ColumnType(0x0008, "float")
case object CqlInt extends ColumnType(0x0009, "int")
case object CqlText extends ColumnType(0x000A, "text")
case object CqlTimestamp extends ColumnType(0x000B, "timestamp")
case object CqlUUID extends ColumnType(0x000C, "uuid")
case object CqlVarchar extends ColumnType(0x000D, "varchar")
case object CqlVarint extends ColumnType(0x000E, "varint")
case object CqlTimeUUID extends ColumnType(0x000F, "timeuuid")
case object CqlInet extends ColumnType(0x0010, "inet")
case object CqlSet extends ColumnType(0x0022, "set")

object ColumnType {
  val ColumnTypeMapping = Map[String, ColumnType](
    CqlInt.stringRep -> CqlInt,
    CqlBoolean.stringRep -> CqlBoolean,
    CqlAscii.stringRep -> CqlAscii,
    CqlBigint.stringRep -> CqlBigint,
    CqlCounter.stringRep -> CqlCounter,
    CqlBlob.stringRep -> CqlBlob,
    CqlDecimal.stringRep -> CqlDecimal,
    CqlDouble.stringRep -> CqlDouble,
    CqlFloat.stringRep -> CqlFloat,
    CqlText.stringRep -> CqlText,
    CqlTimestamp.stringRep -> CqlTimestamp,
    CqlUUID.stringRep -> CqlUUID,
    CqlInet.stringRep -> CqlInet,
    CqlVarint.stringRep -> CqlVarint,
    CqlTimeUUID.stringRep -> CqlTimeUUID,
    CqlSet.stringRep -> CqlSet
  )

  def fromString(string: String) : Option[ColumnType] = {
    ColumnTypeMapping.get(string.toLowerCase( ))
  }
}
