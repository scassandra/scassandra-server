package org.scassandra.cqlmessages

object OpCodes {
  val Register: Byte = 0x0B
  val Error: Byte = 0x00
  val Startup: Byte = 0x01
  val Ready: Byte = 0x02
  val Options: Byte = 0x05
  val Query: Byte = 0x07
  val Result: Byte = 0x08
}