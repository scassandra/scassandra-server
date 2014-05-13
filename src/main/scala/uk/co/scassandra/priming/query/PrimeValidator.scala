package uk.co.scassandra.priming.query

import uk.co.scassandra.cqlmessages._
import java.math.BigDecimal
import java.util.UUID
import java.net.{UnknownHostException, InetAddress}
import scala.collection.immutable.Map

class PrimeValidator {

  def validate(criteria: PrimeCriteria, prime: Prime, queryToResults: Map[PrimeCriteria, Prime]): PrimeAddResult = {
    // 1. Validate consistency
    validateConsistency(criteria, queryToResults) match {
      case c: ConflictingPrimes => c
      // 2. Then validate column types
      case _ => validateColumnTypes(prime)
    }
  }

  private def validateConsistency(criteria: PrimeCriteria, queryToResults: Map[PrimeCriteria, Prime]): PrimeAddResult = {

    def intersectsExistingCriteria: (PrimeCriteria) => Boolean = {
      existing => existing.query == criteria.query && existing.consistency.intersect(criteria.consistency).size > 0
    }

    val intersectingCriteria = queryToResults.filterKeys(intersectsExistingCriteria).keySet.toList
    intersectingCriteria match {
      // exactly one intersecting criteria: if the criteria is the newly passed one, this is just an override. Otherwise, conflict.
      case list@head :: Nil if head != criteria => ConflictingPrimes(list)
      // two or more intersecting criteria: this means one or more conflicts
      case list@head :: second :: rest => ConflictingPrimes(list)
      // all other cases: carry on
      case _ => PrimeAddSuccess
    }
  }

  private def validateColumnTypes(prime: Prime): PrimeAddResult = {
    val typeMismatches = {
      for {
        row <- prime.rows
        (column, value) <- row
        columnType = prime.columnTypes(column)
        if isTypeMismatch(value, columnType)
      } yield {
        TypeMismatch(value, column, columnType.stringRep)
      }
    }

    typeMismatches match {
      case Nil => PrimeAddSuccess
      case l: List[TypeMismatch] => TypeMismatches(typeMismatches)
    }
  }

  private def isTypeMismatch(value: Any, columnType: ColumnType[_]): Boolean = {
    try {
      convertValue(value, columnType)
      false
    } catch {
      case
        _: ClassCastException |
        _: NumberFormatException |
        _: IllegalArgumentException |
        _: StringIndexOutOfBoundsException |
        _: UnknownHostException =>
        true
    }
  }

  private def convertValue(value: Any, columnType: ColumnType[_]): Any = {
    columnType match {
      case CqlVarchar | CqlAscii | CqlText =>
        value.asInstanceOf[String]
      case CqlInt =>
        if (value.isInstanceOf[String]) {
          value.toString.toInt
        } else value.asInstanceOf[Long]
      case CqlBoolean =>
        value.toString.toBoolean
      case CqlBigint | CqlCounter =>
        value.toString.toLong
      case CqlBlob =>
        hex2Bytes(value.toString)
      case CqlDecimal =>
        new BigDecimal(value.toString)
      case CqlDouble =>
        value.toString.toDouble
      case CqlFloat =>
        value.toString.toFloat
      case CqlTimestamp =>
        value.toString.toLong
      case CqlUUID | CqlTimeUUID =>
        UUID.fromString(value.toString)
      case CqlInet =>
        InetAddress.getByName(value.toString)
      case CqlVarint =>
        BigInt(value.toString)
      // TODO - type mismatch of cqlset
      case CqlSet =>
        value.asInstanceOf[Iterable[String]]
    }
  }

  private def hex2Bytes(hex: String): Array[Byte] = {
    (for {i <- 0 to hex.length - 1 by 2 if i > 0 || !hex.startsWith("0x")}
    yield hex.substring(i, i + 2))
      .map(hexValue => Integer.parseInt(hexValue, 16).toByte).toArray
  }
}

abstract class PrimeAddResult

case class TypeMismatches(typeMismatches: List[TypeMismatch]) extends PrimeAddResult

case object PrimeAddSuccess extends PrimeAddResult

case class ConflictingPrimes(existingPrimes: List[PrimeCriteria]) extends PrimeAddResult

case class TypeMismatch(value: Any, name: String, columnType: String)

object PrimeValidator {
  def apply() = {
    new PrimeValidator
  }
}
