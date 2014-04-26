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


      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults)))) ~> route ~> check {
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

      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults)))) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, Success, defaultedColumnTypes))
      }
    }

    it("should populate PrimedResults with ReadTimeout when result is read_request_timeout") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("read_request_timeout")

      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults), result))) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, ReadTimeout))
      }
    }

    it("should populate PrimedResults with WriteTimeout when result is write_request_timeout") {
      val query = "insert into something"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("write_request_timeout")

      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults), result))) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, WriteTimeout))
      }
    }

    it("should populate PrimedResults with Success for result success") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("success")

      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults), result))) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, Success))
      }
    }

    it("should populate PrimedResults with Unavailable for result unavailable") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenResults = List[Map[String, String]]()
      val result = Some("unavailable")

      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults), result))) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenResults, Unavailable))
      }
    }

    it("should delete all primes for a HTTP delete") {
      val whenQuery = PrimeCriteria("anything", List())
      primedResults.add(whenQuery, Prime(List()))

      Delete("/prime") ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)) should equal(None)
      }
    }

    it("should turn handle rejected primes as bad request") {
      val consistencies: List[Consistency] = List(ONE, TWO)
      val query: String = "select * from people"
      primedResults.add(PrimeCriteria(query, consistencies), Prime(List[Map[String, Any]]()))

      val whenQuery = When("select * from people")
      val thenResults = List[Map[String, String]]()
      val result = Some("success")

      Post("/prime", PrimeQueryResult(whenQuery, Then(Some(thenResults), result))) ~> route ~> check {
        status should equal(BadRequest)
        responseAs[ConflictingPrimes] should equal(ConflictingPrimes(existingPrimes = List(PrimeCriteria(query, consistencies))))
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

        Post("/prime", primePayload) ~> route ~> check {
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

        Post("/prime", primePayload) ~> route ~> check {
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

        Post("/prime", primePayload) ~> route ~> check {
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

        Post("/prime", primePayload) ~> route ~> check {
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

      Post("/prime", primePayload) ~> route ~> check {
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

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("age" -> CqlVarchar)))
      }
    }

    it("Should convert boolean to ColumnType CqlBoolean") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("booleanValue" -> "false"))
      val thenColumnTypes = Map("booleanValue" -> "boolean")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("booleanValue" -> CqlBoolean)))
      }
    }

    it("Should convert ascii to ColumnType CqlAscii") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("asciiValue" -> "Hello"))
      val thenColumnTypes = Map("asciiValue" -> "ascii")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("asciiValue" -> CqlAscii)))
      }
    }

    it("Should convert bigint to ColumnType CqlBigint") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "1234"))
      val thenColumnTypes = Map("field" -> "bigint")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlBigint)))
      }
    }

    it("Should convert counter to ColumnType CqlCounter") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "1234"))
      val thenColumnTypes = Map("field" -> "counter")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlCounter)))
      }
    }

    it("Should convert blob to ColumnType CqlBlob") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "0x48656c6c6f"))
      val thenColumnTypes = Map("field" -> "blob")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlBlob)))
      }
    }

    it("Should convert decimal to ColumnType CqlDecimal") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "decimal")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlDecimal)))
      }
    }

    it("Should convert double to ColumnType CqlDouble") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "double")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlDouble)))
      }
    }

    it("Should convert float to ColumnType CqlFloat") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "float")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlFloat)))
      }
    }

    it("Should convert text to ColumnType CqlText") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "text")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlText)))
      }
    }

    it("Should convert timestamp to ColumnType CqlTimestamp") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "timestamp")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlTimestamp)))
      }
    }

    it("Should convert uuid to ColumnType CqlUUID") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "uuid")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlUUID)))
      }
    }

    it("Should convert inet to ColumnType CqlInet") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533.78867"))
      val thenColumnTypes = Map("field" -> "inet")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlInet)))
      }
    }
    it("Should convert varint to ColumnType CqlVarint") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533"))
      val thenColumnTypes = Map("field" -> "varint")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlVarint)))
      }
    }
    it("Should convert timeuuid to ColumnType CqlTimeUUID") {
      val query = "select * from users"
      val whenQuery = When(query)
      val thenRows = List(Map("field" -> "533"))
      val thenColumnTypes = Map("field" -> "timeuuid")
      val primePayload = PrimeQueryResult(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post("/prime", primePayload) ~> route ~> check {
        primedResults.get(PrimeMatch(whenQuery.query, ONE)).get should equal(Prime(thenRows, Success, columnTypes = Map[String, ColumnType]("field" -> CqlTimeUUID)))
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
}
