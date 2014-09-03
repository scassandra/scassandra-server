package org.scassandra.cqlmessages.response

import org.scalatest.{Matchers, FunSuite}
import akka.util.{ByteString, ByteIterator}
import org.scassandra.cqlmessages.types.{CqlVarchar, CqlMap}

class ResultHelperTest extends FunSuite with Matchers { 
  test("Serialising map type") {
    //given
    val byteBuilder = ByteString.newBuilder
    //when
    ResultHelper.serialiseTypeInfomration("map_type", new CqlMap(CqlVarchar, CqlVarchar), byteBuilder)
    //then
    byteBuilder.result() should equal(ByteString(0, 8, 109, 97, 112, 95, 116, 121, 112, 101, 0, 33, 0, 13, 0, 13))
  }  
}
