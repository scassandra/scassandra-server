package uk.co.scassandra.e2e

import uk.co.scassandra.AbstractIntegrationTest
import org.scalatest.concurrent.ScalaFutures
import com.datastax.driver.core.DataType
import java.nio.ByteBuffer
import java.util.{UUID, Date}
import java.net.InetAddress

class PrimingCqlTypesTest extends AbstractIntegrationTest with ScalaFutures {

  test("Priming with missing type defaults to CqlVarchar") {
    // priming
    val whenQuery = "select * from people"
    val rows: List[Map[String, String]] = List(Map("name" -> "Chris", "age" -> "19"))
    val columnTypes: Map[String, String] = Map("name" -> "varchar")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getString("age") should equal("19")
  }

  test("Priming a CQL Text") {
    // priming
    val whenQuery = "select * from people"
    val rows: List[Map[String, String]] = List(Map("name" -> "Chris"))
    val columnTypes: Map[String, String] = Map("name" -> "text")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getColumnDefinitions.getType("name") should equal(DataType.text())

  }

  test("Priming a CQL int") {
    // priming
    val whenQuery = "Test prime and query with a cql int"
    val rows: List[Map[String, String]] = List(Map("age" -> "29"))
    val columnTypes: Map[String, String] = Map("age" -> "int")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getInt("age") should equal(29)

  }

  test("Priming a CQL boolean") {
    // priming
    val whenQuery = "Test prime with cql boolean"
    val rows: List[Map[String, String]] = List(Map("booleanTrue" -> "true", "booleanFalse" -> "false"))
    val columnTypes: Map[String, String] = Map("booleanTrue" -> "boolean", "booleanFalse" -> "boolean")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getBool("booleanTrue") should equal(true)
    results.get(0).getBool("booleanFalse") should equal(false)
  }

  test("Priming a CQL ASCII") {
    // priming
    val whenQuery = "Test prime with cql ascii"
    val rows: List[Map[String, String]] = List(Map("asciiField" -> "Hello There"))
    val columnTypes: Map[String, String] = Map("asciiField" -> "ascii")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("asciiField") should equal("Hello There")
    results.get(0).getColumnDefinitions.getType("asciiField") should equal(DataType.ascii())
  }

  test("Priming a CQL Bigint") {
    // priming
    val whenQuery = "Test prime with cql bigint"
    val rows: List[Map[String, String]] = List(Map("bigIntField" -> "1234"))
    val columnTypes: Map[String, String] = Map("bigIntField" -> "bigint")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getLong("bigIntField") should equal(1234)
    results.get(0).getColumnDefinitions.getType("bigIntField") should equal(DataType.bigint())
  }

  test("Priming a CQL Counter") {
    // priming
    val whenQuery = "Test prime with cql bigint"
    val rows: List[Map[String, String]] = List(Map("field" -> "1234"))
    val columnTypes: Map[String, String] = Map("field" -> "counter")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.counter())
    results.get(0).getLong("field") should equal(1234)
  }

  test("Priming a CQL BLOB") {
    // priming
    val whenQuery = "Test prime with cql blob"
    val rows: List[Map[String, String]] = List(Map("field" -> "0x48656c6c6f"))
    val columnTypes: Map[String, String] = Map("field" -> "blob")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.blob())
    val byteBuffer: ByteBuffer = results.get(0).getBytes("field")
    val bytes = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(bytes)
    bytes should equal(Array[Byte](0x48, 0x65, 0x6c, 0x6c, 0x6f))
  }

  test("Priming a CQL Decimal") {
    // priming
    val whenQuery = "Test prime with cql decimal"
    val rows: List[Map[String, String]] = List(Map("field" -> "4.3456"))
    val columnTypes: Map[String, String] = Map("field" -> "decimal")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.decimal())

    results.get(0).getDecimal("field") should equal(new java.math.BigDecimal("4.3456"))
  }

  test("Priming a CQL Double") {
    // priming
    val whenQuery = "Test prime with cql double"
    val rows: List[Map[String, String]] = List(Map("field" -> "4.3456"))
    val columnTypes: Map[String, String] = Map("field" -> "double")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.cdouble())

    results.get(0).getDouble("field") should equal(4.3456)
  }

  test("Priming a CQL Float") {
    // priming
    val whenQuery = "Test prime with cql double"
    val rows: List[Map[String, String]] = List(Map("field" -> "4.3456"))
    val columnTypes: Map[String, String] = Map("field" -> "float")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.cfloat())

    results.get(0).getFloat("field") should equal(4.3456f)
  }

  test("Priming a CQL Timestamp") {
    // priming
    val date = new Date()
    val whenQuery = "Test prime with cql timestamp"
    val rows: List[Map[String, String]] = List(Map("field" -> s"${date.getTime}"))
    val columnTypes: Map[String, String] = Map("field" -> "timestamp")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.timestamp())

    results.get(0).getDate("field") should equal(date)
  }

  test("Priming a CQL UUID") {
    // priming
    val uuid = UUID.randomUUID()
    val whenQuery = "Test prime with cql uuid"
    val rows: List[Map[String, String]] = List(Map("field" -> s"${uuid.toString}"))
    val columnTypes: Map[String, String] = Map("field" -> "uuid")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.uuid())

    results.get(0).getUUID("field") should equal(uuid)
  }

  test("Priming a CQL inet") {
    // priming
    val inet = InetAddress.getByAddress(Array[Byte](127,0,0,1))
    val whenQuery = "Test prime with cql uuid"
    val rows: List[Map[String, String]] = List(Map("field" -> "127.0.0.1"))
    val columnTypes: Map[String, String] = Map("field" -> "inet")
    prime(whenQuery, rows, "success", columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.inet())

    results.get(0).getInet("field") should equal(inet)
  }
}
