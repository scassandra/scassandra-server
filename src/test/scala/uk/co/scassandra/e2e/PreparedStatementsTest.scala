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

  test("Prepared statement - priming numeric parameters") {
    //given
    val preparedStatementText = "insert into people(bigint, counter, decimal, double, float, int, varint) = (?, ?,?,?,?,?,?)"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(preparedStatementText),
      ThenPreparedSingle(Some(List()), Some(List(CqlBigint, CqlCounter, CqlDecimal, CqlDouble, CqlFloat, CqlInt, CqlVarint)))
    )
    val bigInt : java.lang.Long = 1234
    val counter : java.lang.Long = 2345
    val decimal : java.math.BigDecimal = new java.math.BigDecimal("1")
    val double : java.lang.Double = 1.5
    val float : java.lang.Float = 2.5f
    val int : java.lang.Integer = 3456
    val varint : java.math.BigInteger = new java.math.BigInteger("123")

    //when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(bigInt, counter, decimal, double, float, int, varint)
    val result = session.execute(boundStatement)

    //then
    result.all().size() should equal(0)
  }

  /*
        ASCII     (1,  String.class),
        BIGINT    (2,  Long.class),
        BLOB      (3,  ByteBuffer.class),
        BOOLEAN   (4,  Boolean.class),
        COUNTER   (5,  Long.class),
        DECIMAL   (6,  BigDecimal.class),
        DOUBLE    (7,  Double.class),
        FLOAT     (8,  Float.class),
        INET      (16, InetAddress.class),
        INT       (9,  Integer.class),
        TEXT      (10, String.class),
        TIMESTAMP (11, Date.class),
        UUID      (12, UUID.class),
        VARCHAR   (13, String.class),
        VARINT    (14, BigInteger.class),
        TIMEUUID  (15, UUID.class),
        LIST      (32, List.class),
        SET       (34, Set.class),
        MAP       (33, Map.class),
        CUSTOM    (0,  ByteBuffer.class);

 */
  test("Prepared statement - priming non-numeric parameters") {
    //given
    val preparedStatementText = "insert into people(ascii, blob, boolean, timestamp, uuid, varchar, timeuuid, inet) = (?,?,?,?,?,?,?,?,?)"
    PrimingHelper.primePreparedStatement(
      WhenPreparedSingle(preparedStatementText),
      ThenPreparedSingle(Some(List()), Some(List(CqlAscii, CqlBlob, CqlBoolean, CqlTimestamp, CqlUUID, CqlVarchar, CqlTimeUUID, CqlInet)))
    )
    val ascii : String = "ascii"
    val blob : ByteBuffer = ByteString().toByteBuffer
    val boolean : java.lang.Boolean = true
    val timestamp : java.util.Date = new Date();
    val uuid : UUID = UUID.randomUUID()
    val varchar : String = "varchar"
    val timeuuid : UUID = UUIDs.timeBased()
    val inet : InetAddress = InetAddress.getLocalHost

    //when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(ascii, blob, boolean, timestamp, uuid, varchar, timeuuid, inet)
    val result = session.execute(boundStatement)

    //then
    result.all().size() should equal(0)
  }
}
