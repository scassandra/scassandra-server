package org.scassandra.server.priming

import java.util.UUID

import org.scalatest.{Matchers, WordSpec}
import org.scassandra.codec.datatype._
import org.scassandra.server.actors.priming.PrimeQueryStoreActor._
import org.scassandra.server.priming.json.Success

class PrimeValidatorTest extends WordSpec with Matchers {
  private val someQuery = "select 1"

  "prime validator" must {
    "validate ints" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> 123),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT AN INTEGER!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlInt))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT AN INTEGER!", "hasInvalidValue", CqlInt.stringRep))))
    }

    "allow cql int to be a big decimal" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("hasLongAsInt" -> BigDecimal("5"))
          )),
          result = Some(Success),
          column_types = Some(Map("hasLongAsInt" -> CqlInt))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(PrimeAddSuccess)
    }

    "validate booleans" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "true"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A BOOLEAN!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlBoolean))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A BOOLEAN!", "hasInvalidValue", CqlBoolean.stringRep))))
    }

    "validate big ints" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "12345"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A BIGINT!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Bigint))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A BIGINT!", "hasInvalidValue", Bigint.stringRep))))
    }

    "validate counters" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "1234"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A COUNTER!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Counter))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A COUNTER!", "hasInvalidValue", Counter.stringRep))))
    }

    "validate blobs" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "0x48656c6c6f"),
            Map("name" -> "catbus", "hasInvalidValue" -> false)
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Blob))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch(false, "hasInvalidValue", Blob.stringRep))))
    }

    "validate decimals" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "5.5456"),
            Map("name" -> "catbus", "hasInvalidValue" -> false)
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlDecimal))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch(false, "hasInvalidValue", CqlDecimal.stringRep))))
    }

    "validate doubles" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "5.5456"),
            Map("name" -> "catbus", "hasInvalidValue" -> false)
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlDouble))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch(false, "hasInvalidValue", CqlDouble.stringRep))))
    }

    "validate floats" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "5.5456"),
            Map("name" -> "catbus", "hasInvalidValue" -> false)
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlFloat))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch(false, "hasInvalidValue", CqlFloat.stringRep))))
    }

    "validate timestamps" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "1368438171000"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TIMESTAMP!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Timestamp))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A TIMESTAMP!", "hasInvalidValue", Timestamp.stringRep))))
    }

    "validate uuids" in {
      val uuid = UUID.randomUUID().toString
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> uuid),
            Map("name" -> "catbus", "hasInvalidValue" -> false)
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Uuid))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch(false, "hasInvalidValue", Uuid.stringRep))))
    }

    "validate inets" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "127.0.0.1"),
            Map("name" -> "validIpv6", "hasInvalidValue" -> "::1"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT AN INET!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlInet))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT AN INET!", "hasInvalidValue", CqlInet.stringRep))))
    }

    "validate varints" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "1234"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A VARINT!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Varint))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A VARINT!", "hasInvalidValue", Varint.stringRep))))
    }

    "validate timeuuids" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> "2c530380-b9f9-11e3-850e-338bb2a2e74f"),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TIME UUID!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlTimeuuid))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A TIME UUID!", "hasInvalidValue", CqlTimeuuid.stringRep))))
    }

    "validate lists" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> List(1, 2, 3, 4)),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A LIST!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlList(CqlInt)))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A LIST!", "hasInvalidValue", CqlList(CqlInt).stringRep))))
    }

    "validate sets" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> Set("uno", "dos", "tres")),
            Map("name" -> "listShouldWorkAndTypesShouldBeCoercable", "hasInvalidValue" -> List(1, 2, 3, 4)),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A SET!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlSet(Varchar)))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A SET!", "hasInvalidValue", CqlSet(Varchar).stringRep))))
    }

    "valiate nested collections" in {
      val uuid = UUID.randomUUID().toString
      val mapType = CqlMap(Uuid, CqlList(CqlInt))

      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> Map(uuid -> List(1, 2, 3, 4))),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A MAP!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> mapType))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A MAP!", "hasInvalidValue", mapType.stringRep))))
    }

    "validate tuples" in {
      val uuid = UUID.randomUUID().toString
      val tupleType = Tuple(Uuid, CqlList(CqlInt))
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> (uuid, List(1, 2, 3, 4))),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TUPLE!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> tupleType))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A TUPLE!", "hasInvalidValue", tupleType.stringRep))))
    }

    "validate times" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> 1368438171000L),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TIME!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlTime))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A TIME!", "hasInvalidValue", CqlTime.stringRep))))
    }

    "validate dates" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> 2147484648L),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A DATE!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> CqlDate))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A DATE!", "hasInvalidValue", CqlDate.stringRep))))
    }

    "validate small ints" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> 512),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A SMALLINT!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Smallint))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A SMALLINT!", "hasInvalidValue", Smallint.stringRep))))
    }

    "validate tinyints" in {
      val prime = PrimeQuerySingle(
        When(
          Some(someQuery)
        ),
        Then(
          rows = Some(List(
            Map("name" -> "totoro", "hasInvalidValue" -> 127),
            Map("name" -> "catbus", "hasInvalidValue" -> "NOT A TINYINT!")
          )),
          result = Some(Success),
          column_types = Some(Map("name" -> Varchar, "hasInvalidValue" -> Tinyint))
        )
      )

      val validationResult = PrimeValidator.validateColumnTypes(prime.prime)
      validationResult should equal(TypeMismatches(List(TypeMismatch("NOT A TINYINT!", "hasInvalidValue", Tinyint.stringRep))))
    }
  }
}
