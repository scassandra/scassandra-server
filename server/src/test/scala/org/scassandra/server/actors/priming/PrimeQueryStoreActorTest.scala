package org.scassandra.server.actors.priming

import akka.Done
import akka.actor.Props
import akka.testkit.ImplicitSender
import org.scalatest.{Matchers, WordSpec}
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.datatype._
import org.scassandra.codec.messages.ColumnSpec.column
import org.scassandra.codec.messages.{QueryParameters, Row, RowMetadata}
import org.scassandra.codec.{Consistency, Query, Rows, SetKeyspace}
import org.scassandra.server.actors.TestKitWithShutdown
import org.scassandra.server.actors.priming.PrimeQueryStoreActor._
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.routes.PrimingJsonHelper

class PrimeQueryStoreActorTest extends WordSpec with TestKitWithShutdown with ImplicitSender with Matchers {

  // An example prime reused throughout tests.
  val someQuery = "some query"
  private val matchingParams = QueryParameters(consistency = Consistency.ONE)
  val matchingQuery = Query(someQuery, matchingParams)
  val someThen = Then(
    rows = Some(List(
      Map("name" -> "Mickey", "age" -> 99),
      Map("name" -> "Mario", "age" -> 12)
    )),
    result = Some(Success),
    column_types = Some(Map("name" -> Varchar, "age" -> CqlInt))
  )
  val somePrime = PrimeQuerySingle(
    When(
      Some(someQuery),
      consistency = Some(List(Consistency.ONE))
    ),
    someThen
  )

  def withConsistency(consistency: Consistency*): PrimeQuerySingle = {
    somePrime.copy(when = somePrime.when.copy(consistency = Some(consistency.toList)))
  }

  // What we expect the Prime message to be.
  val someRows = Rows(
    RowMetadata(keyspace = Some(""), table = Some(""), columnSpec = Some(List(column("name", Varchar), column("age", CqlInt)))),
    List(Row("name" -> "Mickey", "age" -> 99), Row("name" -> "Mario", "age" -> 12))
  )

  "prime query store" must {
    val underTest = system.actorOf(Props[PrimeQueryStoreActor])
    "add a prime" in {
      underTest ! RecordQueryPrime(somePrime)
      expectMsg(PrimeAddSuccess)
    }

    "return the prime if query matches" in {
      underTest ! MatchPrime(matchingQuery)
      expectMsgPF() {
        case MatchResult(Some(Reply(rows, _, _))) if rows == someRows => true
      }
    }

    "return nil if no results for a given query" in {
      underTest ! MatchPrime(Query("any old query"))
      expectMsg(MatchResult(None))
    }

    "return set keyspace for use queries" in {
      val prefixes = List("use", " USE", "uSE  ")
      prefixes.foreach { prefix =>
        underTest ! MatchPrime(Query(s"$prefix someKeyspace"))
        expectMsgPF() { case MatchResult(Some(Reply(SetKeyspace(ks), _, _))) if ks == "someKeyspace" => }
      }
    }

    "clear on ClearPrimes" in {
      underTest ! ClearQueryPrimes
      expectMsg(Done)
      underTest ! MatchPrime(matchingQuery)
      expectMsg(MatchResult(None))
    }

    "match on all parts of the query" must {
      val underTest = system.actorOf(Props[PrimeQueryStoreActor])
      underTest ! RecordQueryPrime(somePrime)
      expectMsg(PrimeAddSuccess)

      "match on consistency" in {
        underTest ! MatchPrime(matchingQuery.copy(parameters = matchingParams.copy(consistency = Consistency.ALL)))
        expectMsg(MatchResult(None))
      }
    }

    "deal with conflicting primes" in {
      val underTest = system.actorOf(Props[PrimeQueryStoreActor])
      val primeForTwoAndAny = withConsistency(Consistency.TWO, Consistency.ANY)
      val primeForThreeAndAny = withConsistency(Consistency.THREE, Consistency.ANY)

      underTest ! RecordQueryPrime(primeForTwoAndAny)
      expectMsg(PrimeAddSuccess)

      underTest ! RecordQueryPrime(primeForThreeAndAny)
      expectMsg(ConflictingPrimes(List(PrimingJsonHelper.extractPrimeCriteria(primeForTwoAndAny).get)))
    }

    "not allow pattern and query pattern" in {
      val underTest = system.actorOf(Props[PrimeQueryStoreActor])
      val prime = PrimeQuerySingle(When(query = Some("something"), queryPattern = Some("something")), Then())
      underTest ! RecordQueryPrime(prime)

      expectMsg(BadCriteria("Can't specify query and queryPattern"))
    }

    "override primes with the same criteria" in {
      val underTest = system.actorOf(Props[PrimeQueryStoreActor])
      val primeForTwoAndAny = withConsistency(Consistency.TWO, Consistency.ANY)
      val primeForTwoAndAnyAgain = primeForTwoAndAny.copy(thenDo = primeForTwoAndAny.thenDo.copy(rows = None))

      underTest ! RecordQueryPrime(primeForTwoAndAny)
      expectMsg(PrimeAddSuccess)
      underTest ! RecordQueryPrime(primeForTwoAndAnyAgain)
      expectMsg(PrimeAddSuccess)

      underTest ! MatchPrime(Query(someQuery, QueryParameters(consistency = Consistency.ANY)))
      expectMsg(MatchResult(Some(primeForTwoAndAnyAgain.prime)))
    }

    "support primes with the same query but different consistency" in {
      val underTest = system.actorOf(Props[PrimeQueryStoreActor])
      val primeCriteriaForONE = withConsistency(Consistency.ONE)
      val primeCriteriaForTWO = withConsistency(Consistency.TWO)
      val primeCriteriaForTHREE = withConsistency(Consistency.THREE)

      underTest ! RecordQueryPrime(primeCriteriaForONE)
      expectMsg(PrimeAddSuccess)
      underTest ! RecordQueryPrime(primeCriteriaForTWO)
      expectMsg(PrimeAddSuccess)
      underTest ! RecordQueryPrime(primeCriteriaForTHREE)
      expectMsg(PrimeAddSuccess)

      underTest ! GetAllPrimes

      expectMsg(AllPrimes(List(primeCriteriaForONE, primeCriteriaForTWO, primeCriteriaForTHREE)))
    }

    "have query patterns that" must {
      "support .* as a wild card" in {
        val underTest = system.actorOf(Props[PrimeQueryStoreActor])

        val queryWithPattern = PrimeQuerySingle(When(queryPattern = Some(".*")), someThen)
        underTest ! RecordQueryPrime(queryWithPattern)
        expectMsg(PrimeAddSuccess)

        underTest ! MatchPrime(Query("anything"))
        expectMsg(MatchResult(Some(queryWithPattern.prime)))
      }

      "support .+ as a wild card, no match" in {
        val underTest = system.actorOf(Props[PrimeQueryStoreActor])
        val query = PrimeQuerySingle(When(queryPattern = Some("hello .+")), someThen)
        underTest ! RecordQueryPrime(query)
        expectMsg(PrimeAddSuccess)

        underTest ! MatchPrime(Query("hello "))
        expectMsg(MatchResult(None))
      }

      "support .+ as a wild card, match" in {
        val underTest = system.actorOf(Props[PrimeQueryStoreActor])
        val query = PrimeQuerySingle(When(queryPattern = Some("hello .+")), someThen)
        underTest ! RecordQueryPrime(query)
        expectMsg(PrimeAddSuccess)

        underTest ! MatchPrime(Query("hello there"))
        expectMsg(MatchResult(Some(query.prime)))
      }
    }
  }

}
