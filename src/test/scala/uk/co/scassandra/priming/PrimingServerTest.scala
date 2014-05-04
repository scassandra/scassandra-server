package uk.co.scassandra.priming

import org.scalatest._

import spray.http.StatusCodes._
import spray.testkit.ScalatestRouteTest
import spray.json._
import uk.co.scassandra.cqlmessages._
import scala.Some

class PrimingServerTest extends FunSpec with BeforeAndAfter with Matchers with ScalatestRouteTest with PrimingServerRoute {

  import JsonImplicits._

  implicit def actorRefFactory = system

  implicit val primedResults = PrimedResults()

  after {
    primedResults.clear()
  }

  describe("Priming") {

    it("should return all primes for get") {
      Get("/prime-single") ~> route ~> check {
        status should equal(OK)
      }
    }

    it("should return OK on valid request") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults =
        List(
          Map(
            "name" -> "Mickey",
            "age" -> "99"
          ),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )


      Post("/prime-single", PrimeQueryResult(whenQuery, Then(Some(thenResults)))) ~> route ~> check {
        status should equal(OK)
      }
    }

    it("should populate PrimedResults on valid request") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults =
        List(
          Map(
            "name" -> "Mickey",
            "age" -> "99"
          ),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )
      val defaultedColumnTypes = Map[String, ColumnType]("name" -> CqlVarchar, "age" -> CqlVarchar)

      Post("/prime-single", PrimeQueryResult(whenQuery, Then(Some(thenResults)))) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, Success, defaultedColumnTypes))
      }
    }

    it("should populate PrimedResults with ReadTimeout when result is read_request_timeout") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("read_request_timeout")

      Post("/prime-single", PrimeQueryResult(whenQuery, Then(Some(thenResults), result))) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, ReadTimeout))
      }
    }

    it("should populate PrimedResults with WriteTimeout when result is write_request_timeout") {
      val query = "insert into something"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("write_request_timeout")

      Post("/prime-single", PrimeQueryResult(whenQuery, Then(Some(thenResults), result))) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, WriteTimeout))
      }
    }

    it("should populate PrimedResults with Success for result success") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("success")

      Post("/prime-single", PrimeQueryResult(whenQuery, Then(Some(thenResults), result))) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, Success))
      }
    }

    it("should populate PrimedResults with Unavailable for result unavailable") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("unavailable")

      Post("/prime-single", PrimeQueryResult(whenQuery, Then(Some(thenResults), result))) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, Unavailable))
      }
    }

    it("should delete all primes for a HTTP delete") {
      val whenQuery = PrimeCriteria("anything", List())
      primedResults.add(whenQuery, Prime(List()))

      Delete("/prime-single") ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)) should equal(None)
      }
    }
  }

  describe("Priming incorrectly") {

    it("should reject conflicting primes as bad request") {
      val consistencies: List[Consistency] = List(ONE, TWO)
      val query: String = "select * from people"
      primedResults.add(PrimeCriteria(query, consistencies), Prime(List[Map[String, Any]]()))

      val whenQuery = When("select * from people")
      val thenResults = List[Map[String, String]]()
      val result = Some("success")

      Post("/prime-single", PrimeQueryResult(whenQuery, Then(Some(thenResults), result))) ~> route ~> check {
        status should equal(BadRequest)
        responseAs[ConflictingPrimes] should equal(ConflictingPrimes(existingPrimes = List(PrimeCriteria(query, consistencies))))
      }
    }

    // TODO [DN|27-04-2014] - remove ignore flag and implement
    ignore("should reject type mismatch in primes as bad request") {
      val when = When("select * from people")

      val thenResults: List[Map[String, String]] =
        List(
          Map(
            "name" -> "Mickey",
            "age" -> "99"
          ),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )
      val thenColumnTypes = Map("age" -> "boolean")

      val then = Then(Some(thenResults), Some("success"), Some(thenColumnTypes))

      Post("/prime-single", PrimeQueryResult(when, then)) ~> route ~> check {
        status should equal(BadRequest)
        responseAs[TypeMismatch] should equal(TypeMismatch("99", "age", "boolean"))
      }
    }
  }

  describe("Setting optional values in 'when'") {
    describe("keyspace") {
      it("should correctly populate PrimedResults with empty string if keyspace name not set") {
        val query = "select * from users"
        val whenQuery = When(query)
        val thenRows = List()

        val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows)))

        Post("/prime-single", primePayload) ~> route ~> check {
          val prime = primedResults.get(PrimeMatch(query, ONE)).get
          prime.keyspace should equal("")
        }

      }

      it("should correctly populate PrimedResults if keyspace name is set") {
        val expectedKeyspace = "mykeyspace"
        val query = "select * from users"
        val whenQuery = When(query, keyspace = Some(expectedKeyspace))
        val thenRows = List()

        val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows)))

        Post("/prime-single", primePayload) ~> route ~> check {
          val prime = primedResults.get(PrimeMatch(query, ONE)).get
          prime.keyspace should equal(expectedKeyspace)
        }

      }
    }

    describe("table") {
      it("should correctly populate PrimedResults with empty string if table name not set") {
        val query = "select * from users"
        val whenQuery = When(query)
        val thenRows = List()

        val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows)))

        Post("/prime-single", primePayload) ~> route ~> check {
          val prime = primedResults.get(PrimeMatch(query, ONE)).get
          prime.table should equal("")
        }

      }

      it("should correctly populate PrimedResults if table name is set") {
        val expectedTable = "mytable"
        val query = "select * from users"
        val whenQuery = When(query, table = Some(expectedTable))
        val thenRows = List()

        val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows)))

        Post("/prime-single", primePayload) ~> route ~> check {
          val prime = primedResults.get(PrimeMatch(query, ONE)).get
          prime.table should equal(expectedTable)
        }

      }
    }

  }

  describe("Priming of types") {
    it("Should convert int to ColumnType CqlInt") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows =
        List(
          Map(
            "age" -> "99"
          ),
          Map(
            "age" -> "12"
          )
        )
      val thenColumnTypes = Map("age" -> "int")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("age" -> CqlInt)))
      }
    }

    it("Should default column types to CqlVarchar") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows =
        List(
          Map(
            "age" -> "99"
          ),
          Map(
            "age" -> "12"
          )
        )
      val thenColumnTypes = Map[String, String]()
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("age" -> CqlVarchar)))
      }
    }

    it("Should convert boolean to ColumnType CqlBoolean") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("booleanValue" -> "false"))
      val thenColumnTypes = Map("booleanValue" -> "boolean")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("booleanValue" -> CqlBoolean)))
      }
    }

    it("Should convert ascii to ColumnType CqlAscii") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("asciiValue" -> "Hello"))
      val thenColumnTypes = Map("asciiValue" -> "ascii")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("asciiValue" -> CqlAscii)))
      }
    }

    it("Should convert bigint to ColumnType CqlBigint") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "1234"))
      val thenColumnTypes = Map("field" -> "bigint")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlBigint)))
      }
    }

    it("Should convert counter to ColumnType CqlCounter") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "1234"))
      val thenColumnTypes = Map("field" -> "counter")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlCounter)))
      }
    }

    it("Should convert blob to ColumnType CqlBlob") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "0x48656c6c6f"))
      val thenColumnTypes = Map("field" -> "blob")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlBlob)))
      }
    }

    it("Should convert decimal to ColumnType CqlDecimal") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "decimal")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlDecimal)))
      }
    }

    it("Should convert double to ColumnType CqlDouble") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "double")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlDouble)))
      }
    }

    it("Should convert float to ColumnType CqlFloat") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "float")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlFloat)))
      }
    }

    it("Should convert text to ColumnType CqlText") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "text")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlText)))
      }
    }

    it("Should convert timestamp to ColumnType CqlTimestamp") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "timestamp")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlTimestamp)))
      }
    }

    it("Should convert uuid to ColumnType CqlUUID") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "uuid")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlUUID)))
      }
    }

    it("Should convert inet to ColumnType CqlInet") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "inet")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlInet)))
      }
    }
    it("Should convert varint to ColumnType CqlVarint") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533"))
      val thenColumnTypes = Map("field" -> "varint")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlVarint)))
      }
    }
    it("Should convert timeuuid to ColumnType CqlTimeUUID") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533"))
      val thenColumnTypes = Map("field" -> "timeuuid")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlTimeUUID)))
      }
    }
    it("Should convert set to ColumnType CqlSet") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533"))
      val thenColumnTypes = Map("field" -> "set")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlSet)))
      }
    }
    it("Should convert types case insensitive") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533"))
      val thenColumnTypes = Map("field" -> "SET")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime-single", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlSet)))
      }
    }
  }

  describe("Retrieving of primes") {
    it("should convert a prime back to the original JSON format") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = Some(List(Map("field" -> "533")))
      val thenColumnTypes = Some(Map("field" -> "timeuuid"))
      val primePayload = PrimeQueryResult(whenQuery, Then(thenRows, column_types = thenColumnTypes))
      val expectedPrimePayloadWithDefaults = createPrimeQueryResultWithDefaults(query, thenRows = thenRows, columnTypes = thenColumnTypes)

      Post("/prime-single", primePayload) ~> route

      Get("/prime-single") ~> route ~> check {
        val response = responseAs[List[PrimeQueryResult]]
        response.size should equal(1)
        response(0) should equal(expectedPrimePayloadWithDefaults)
      }
    }

    it ("should convert result back to original JSON format") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = Some(List(Map("one"->"two")))
      val result = "read_request_timeout"
      val primePayload = PrimeQueryResult(whenQuery, Then(thenRows, Some(result)))
      val expectedPrimePayloadWithDefaults = createPrimeQueryResultWithDefaults(query, thenRows = thenRows, result = result)
      Post("/prime-single", primePayload) ~> route

      Get("/prime-single") ~> route ~> check {
        val response = responseAs[List[PrimeQueryResult]]
        response.size should equal(1)
        response(0) should equal(expectedPrimePayloadWithDefaults)
      }
    }


    it("Should return keyspace passed into prime") {
      val query = "select * from users"
      val whenQuery = When(query, keyspace = Some("myKeyspace"))
      val thenRows = Some(List(Map("one"->"two")))
      val columnTypes = Map("one"->"varchar")
      val primePayload = PrimeQueryResult(whenQuery, Then(thenRows, column_types =  Some(columnTypes)))
      val expectedPrimePayloadWithDefaults = createPrimeQueryResultWithDefaults(query, thenRows = thenRows, keyspace = "myKeyspace")

      Post("/prime-single", primePayload) ~> route

      Get("/prime-single") ~> route ~> check {
        val response = responseAs[List[PrimeQueryResult]]
        response.size should equal(1)
        response(0) should equal(expectedPrimePayloadWithDefaults)
      }
    }
    it("Should return table passed into prime") {
      val query = "select * from users"
      val whenQuery = When(query, table = Some("tablename"))
      val thenRows = Some(List(Map("one"->"two")))
      val primePayload = PrimeQueryResult(whenQuery, Then(thenRows))
      val expectedPrimePayloadWithDefaults = createPrimeQueryResultWithDefaults(query, thenRows = thenRows, table = "tablename")

      Post("/prime-single", primePayload) ~> route

      Get("/prime-single") ~> route ~> check {
        val response = responseAs[List[PrimeQueryResult]]
        response.size should equal(1)
        response(0) should equal(expectedPrimePayloadWithDefaults)
      }
    }

    it("Should return consistencies passed into prime") {
      val query = "select * from users"
      val consistencies = Some(List("ONE", "ALL"))
      val whenQuery = When(query, consistency = consistencies)
      val thenRows = Some(List(Map("one"->"two")))
      val primePayload = PrimeQueryResult(whenQuery, Then(thenRows))
      val expectedPrimePayloadWithDefaults = createPrimeQueryResultWithDefaults(query, thenRows = thenRows, consistencies = consistencies)

      Post("/prime-single", primePayload) ~> route

      Get("/prime-single") ~> route ~> check {
        val response = responseAs[List[PrimeQueryResult]]
        response.size should equal(1)
        response(0) should equal(expectedPrimePayloadWithDefaults)
      }
    }


  }

  describe("Retrieving activity") {
    it("Should return connection count from ActivityLog for single connection") {
      ActivityLog.clearConnections()
      ActivityLog.recordConnection()

      Get("/connection") ~> route ~> check {
        val response: String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(1)
      }
    }

    it("Should return connection count from ActivityLog for no connections") {
      ActivityLog.clearConnections()

      Get("/connection") ~> route ~> check {
        val response: String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(0)
      }
    }

    it("Should clear connections for a delete") {
      ActivityLog.recordConnection()

      Delete("/connection") ~> route ~> check {
        ActivityLog.retrieveConnections().size should equal(0)
      }
    }

    it("Should return queries from ActivityLog - no queries") {
      ActivityLog.clearQueries()

      Get("/query") ~> route ~> check {
        val response: String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList.size should equal(0)
      }
    }

    it("Should return queries from ActivityLog - single query") {
      ActivityLog.clearQueries()
      val query: String = "select * from people"
      ActivityLog.recordQuery(query, ONE)

      Get("/query") ~> route ~> check {
        val response: String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList.size should equal(1)
        queryList(0).query should equal(query)
      }
    }

    it("Should clear queries for a delete") {
      ActivityLog.recordQuery("select * from people", ONE)

      Delete("/query") ~> route ~> check {
        ActivityLog.retrieveQueries().size should equal(0)
      }
    }
  }


  private def createPrimeQueryResultWithDefaults(query: String,
                                                 keyspace : String = "",
                                                 table : String = "",
                                                 thenRows : Option[List[Map[String, String]]] = None,
                                                 result : String = "success",
                                                 columnTypes : Option[Map[String, String]] = None,
                                                 consistencies : Option[List[String]] = None) = {


    val consistenciesDefaultingToAll = if (consistencies.isDefined) {
      consistencies.get
    } else {
      Consistency.all.map(_.string)
    }
    val colTypesDefaultingToVarchar = if (columnTypes.isDefined) {
      columnTypes.get
    } else {
      // check that all the columns in the rows have a type
      val columnNamesInAllRows = thenRows.get.flatMap(row => row.keys).distinct
      columnNamesInAllRows.map(colName => (colName, "varchar")).toMap
    }
    PrimeQueryResult(When(query, keyspace = Some(keyspace), table = Some(table), consistency = Some(consistenciesDefaultingToAll)), Then(thenRows, Some(result), Some(colTypesDefaultingToVarchar)))
  }
}
