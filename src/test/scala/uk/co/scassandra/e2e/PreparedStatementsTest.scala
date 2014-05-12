package uk.co.scassandra.e2e

import uk.co.scassandra.{PrimingHelper, AbstractIntegrationTest}
import uk.co.scassandra.priming.prepared.{ThenPreparedSingle, WhenPreparedSingle}
import uk.co.scassandra.cqlmessages._
import uk.co.scassandra.priming.prepared.ThenPreparedSingle
import uk.co.scassandra.priming.prepared.WhenPreparedSingle
import scala.Some
import java.math.BigInteger
import java.nio.ByteBuffer
import akka.util.ByteString
import java.util.{UUID, Date}
import com.datastax.driver.core.utils.UUIDs
import java.net.InetAddress
import java.util
import com.datastax.driver.core.Row
import uk.co.scassandra.cqlmessages.response.ReadRequestTimeout
import uk.co.scassandra.priming.{Unavailable, WriteTimeout, ReadTimeout}
import com.datastax.driver.core.exceptions.{UnavailableException, WriteTimeoutException, ReadTimeoutException}

class PreparedStatementsTest extends AbstractIntegrationTest {
  test("Prepared statement without priming - no params") {
    //given
    val preparedStatement = session.prepare("select * from people")
    val boundStatement = preparedStatement.bind()

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement without priming - single params") {
    //given
    val preparedStatement = session.prepare("select * from people where name = ?")
    val boundStatement = preparedStatement.bind("name")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement for schema change") {
    //given
    val preparedStatement = session.prepare("CREATE KEYSPACE ? WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ?, 'dc2': ?};")
    val boundStatement = preparedStatement.bind("keyspaceName","3","1")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement with priming - empty rows") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(preparedStatementText),
      ThenPreparedSingle(Some(List()))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement with priming - read_request_timeout") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(preparedStatementText),
      ThenPreparedSingle(None, result = Some(ReadTimeout))
    )
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    intercept[ReadTimeoutException] {
      session.execute(boundStatement)
    }
  }

  test("Prepared statement with priming - write_request_timeout") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(preparedStatementText),
      ThenPreparedSingle(None, result = Some(WriteTimeout))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    intercept[WriteTimeoutException] {
      session.execute(boundStatement)
    }
  }

  test("Prepared statement with priming - unavailable") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(preparedStatementText),
      ThenPreparedSingle(None, result = Some(Unavailable))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    intercept[UnavailableException] {
      session.execute(boundStatement)
    }
  }

  test("Prepared statement with priming - single row") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(preparedStatementText),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }

  ignore("Type mis-match exceptions") {}

  test("Prepared statement - priming numeric parameters") {
    //given
    val preparedStatementText = "insert into people(bigint, counter, decimal, double, float, int, varint) = (?, ?,?,?,?,?,?)"
    val resultColumnTypes = Map("bigint" -> CqlBigint,
      "counter" -> CqlCounter,
      "decimal" -> CqlDecimal,
      "double" -> CqlDouble,
      "float" -> CqlFloat,
      "int" -> CqlInt,
      "varint" -> CqlVarint)
    val bigInt : java.lang.Long = 1234
    val counter : java.lang.Long = 2345
    val decimal : java.math.BigDecimal = new java.math.BigDecimal("1")
    val double : java.lang.Double = 1.5
    val float : java.lang.Float = 2.5f
    val int : java.lang.Integer = 3456
    val varint : java.math.BigInteger = new java.math.BigInteger("123")

    val rows: List[Map[String, Any]] = List(
      Map(
        "bigint" -> bigInt.toString,
        "counter" -> counter.toString,
        "decimal" -> decimal.toString,
        "double" -> double.toString,
        "float" -> float.toString,
        "int" -> int.toString,
        "varint" -> varint.toString
      )
    )
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(preparedStatementText),
      ThenPreparedSingle(Some(rows),
        Some(List(CqlBigint, CqlCounter, CqlDecimal, CqlDouble, CqlFloat, CqlInt, CqlVarint)),
        Some(resultColumnTypes))
    )

    //when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(bigInt, counter, decimal, double, float, int, varint)
    val result = session.execute(boundStatement)

    //then
    val all: util.List[Row] = result.all()
    all.size() should equal(1)
    val resultRow = all.get(0)
    resultRow.getLong("bigint") should equal(bigInt)
    resultRow.getLong("counter") should equal(counter)
    resultRow.getDecimal("decimal") should equal(decimal)
    resultRow.getDouble("double") should equal(double)
    resultRow.getFloat("float") should equal(float)
    resultRow.getInt("int") should equal(int)
    resultRow.getVarint("varint") should equal(varint)
  }

  test("Prepared statement - priming non-numeric parameters") {
    //given
    val preparedStatementText = "insert into people(ascii, blob, boolean, timestamp, uuid, varchar, timeuuid, inet) = (?,?,?,?,?,?,?,?,?)"
    val resultColumnTypes = Map(
      "ascii" -> CqlAscii,
      "blob" -> CqlBlob,
      "boolean" -> CqlBoolean,
      "timestamp" -> CqlTimestamp,
      "uuid" -> CqlUUID,
      "varchar" -> CqlVarchar,
      "timeuuid" -> CqlTimeUUID,
      "inet" -> CqlInet
    )

    val ascii : String = "ascii"
    val blob : ByteBuffer = ByteString().toByteBuffer
    val boolean : java.lang.Boolean = true
    val timestamp : java.util.Date = new Date();
    val uuid : UUID = UUID.randomUUID()
    val varchar : String = "varchar"
    val timeuuid : UUID = UUIDs.timeBased()
    val inet : InetAddress = InetAddress.getLocalHost

    val primedRow = Map(
      "ascii" -> ascii.toString,
      "blob" -> "0x",
      "boolean" -> boolean,
      "timestamp" -> timestamp.getTime,
      "uuid" -> uuid.toString,
      "varchar" -> varchar,
      "timeuuid" -> timeuuid.toString,
      "inet" -> "127.0.0.1"
    )

    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(preparedStatementText),
      ThenPreparedSingle(Some(List(primedRow)),
        Some(List(CqlAscii, CqlBlob, CqlBoolean, CqlTimestamp, CqlUUID, CqlVarchar, CqlTimeUUID, CqlInet)),
        column_types = Some(resultColumnTypes))
    )

    //when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(ascii, blob, boolean, timestamp, uuid, varchar, timeuuid, inet)
    val result = session.execute(boundStatement)

    //then
    val all: util.List[Row] = result.all()
    all.size() should equal(1)
    val resultRow = all.get(0)

    resultRow.getString("ascii") should equal(ascii)
    resultRow.getBytes("blob") should equal(blob)
    resultRow.getBool("boolean") should equal(boolean)
    resultRow.getDate("timestamp") should equal(timestamp)
    resultRow.getUUID("uuid") should equal(uuid)
    resultRow.getString("varchar") should equal(varchar)
    resultRow.getUUID("timeuuid") should equal(timeuuid)
    resultRow.getInet("inet") should equal(inet)
  }
}
