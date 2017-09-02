package org.scassandra.server.actors.priming

import java.util.regex.Pattern

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.io.Tcp
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.{Message, Query, SetKeyspace}
import org.scassandra.server.actors.priming.PrimeQueryStoreActor._
import org.scassandra.server.priming.Defaulter._
import org.scassandra.server.priming.PrimeValidator
import org.scassandra.server.priming.json.ResultJsonRepresentation
import org.scassandra.server.priming.routes.PrimingJsonHelper
import org.scassandra.server.priming.routes.PrimingJsonHelper.extractPrime

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class PrimeQueryStoreActor extends Actor with ActorLogging {

  import context._

  def receive = currentPrimes(Map(), Map())
  private val useKeyspace: Pattern = Pattern.compile("\\s*use\\s+(.*)$", Pattern.CASE_INSENSITIVE)

  def currentPrimes(queryPrimes: Map[PrimeCriteria, PrimeQuerySingle], queryPatternPrimes: Map[PrimeCriteria, PrimeQuerySingle]): Receive = {
    case RecordQueryPrime(pqs) =>
      val p: PrimeQuerySingle = pqs.withDefaults
      PrimingJsonHelper.extractPrimeCriteria(p) match {
        case Success(criteria) =>
          PrimeValidator.validate(criteria, p.prime, queryPrimes.keys.toList) match {
            case PrimeAddSuccess =>
              if (criteria.patternMatch) {
                sender() ! PrimeAddSuccess
                become(currentPrimes(queryPrimes, queryPatternPrimes + (criteria -> p)))
              } else {
                sender() ! PrimeAddSuccess
                become(currentPrimes(queryPrimes + (criteria -> p), queryPatternPrimes))
              }
            case notSuccess: PrimeAddResult =>
              sender() ! notSuccess
          }
        case Failure(x) =>
          log.warning(s"Received invalid prime", x)
          sender() ! BadCriteria(x.getMessage)
      }
    case MatchPrime(q) =>
      def findPrime: ((PrimeCriteria, PrimeQuerySingle)) => Boolean = {
        entry =>
          entry._1.query == q.query &&
            entry._1.consistency.contains(q.parameters.consistency)
      }
      def findPrimePattern: ((PrimeCriteria, PrimeQuerySingle)) => Boolean = {
        entry => {
          entry._1.query.r.findFirstIn(q.query) match {
            case Some(_) => entry._1.consistency.contains(q.parameters.consistency)
            case None => false
          }
        }
      }

      val keyspaceMatcher = useKeyspace.matcher(q.query)
      val prime = if (keyspaceMatcher.matches()) {
        val keyspaceName = keyspaceMatcher.group(1)
        Some(Reply(SetKeyspace(keyspaceName)))
      } else {
        queryPrimes.find(findPrime).orElse(queryPatternPrimes.find(findPrimePattern)).map(_._2.prime)
      }
      sender() ! MatchResult(prime)
    case ClearQueryPrimes =>
      sender() ! Done
      become(currentPrimes(Map(), Map()))
    case GetAllPrimes =>
      sender() ! AllPrimes(queryPrimes.values.toList)

  }

}

object PrimeQueryStoreActor {
  case class RecordQueryPrime(primeQuerySingle: PrimeQuerySingle)
  case class MatchPrime(query: Query)
  case class MatchResult(prime: Option[Prime])
  case object ClearQueryPrimes
  case object GetAllPrimes
  case class AllPrimes(all: List[PrimeQuerySingle])

  sealed trait PrimeAddResult
  case object PrimeAddSuccess extends PrimeAddResult
  case class TypeMismatches(typeMismatches: List[TypeMismatch]) extends PrimeAddResult
  case class ConflictingPrimes(existingPrimes: List[PrimeCriteria]) extends PrimeAddResult
  case class BadCriteria(message: String) extends PrimeAddResult

  case class TypeMismatch(value: Any, name: String, columnType: String)

  trait ThenProvider {
    val rows: Option[List[Map[String, Any]]]
    val result: Option[ResultJsonRepresentation]
    val column_types: Option[Map[String, DataType]]
    val fixedDelay: Option[Long]
    val config: Option[Map[String, String]]
  }

  case class Then(rows: Option[List[Map[String, Any]]] = None,
                  result: Option[ResultJsonRepresentation] = None,
                  column_types: Option[Map[String, DataType]] = None,
                  fixedDelay: Option[Long] = None,
                  config: Option[Map[String, String]] = None,
                  variable_types: Option[List[DataType]] = None) extends ThenProvider {

    def withDefaults(query: Option[String]): Then =
      copy(
        variable_types = defaultVariableTypesToVarChar(query, variable_types),
        column_types = defaultColumnTypesToVarChar(column_types, rows)
      )
  }
  case class When(query: Option[String] = None,
                  queryPattern: Option[String] = None,
                  consistency: Option[List[Consistency]] = None,
                  keyspace: Option[String] = None,
                  table: Option[String] = None
                 ) {
    def withDefaults: When = copy(consistency = defaultConsistency(consistency))
  }

  case class PrimeQuerySingle(when: When, thenDo: Then) {
    @transient lazy val prime = {
      extractPrime(thenDo, when.keyspace, when.table)
    }

    def withDefaults: PrimeQuerySingle = {
      PrimeQuerySingle(
        when.withDefaults,
        thenDo.withDefaults(when.query)
      )
    }


  }

  case class PrimeCriteria(query: String, consistency: List[Consistency], patternMatch: Boolean = false)

  sealed trait Prime {
    val fixedDelay: Option[FiniteDuration]
    val variableTypes: Option[List[DataType]]
  }


  case class Reply(message: Message, fixedDelay: Option[FiniteDuration] = None, variableTypes: Option[List[DataType]] = None) extends Prime

  sealed trait Fatal extends Prime {
    def produceFatalError(tcpConnection: ActorRef)
  }

  case class ClosedConnectionReport(command: String, fixedDelay: Option[FiniteDuration] = None, variableTypes: Option[List[DataType]] = None) extends Fatal {

    private lazy val closeCommand: Tcp.CloseCommand = command match {
      case "reset" => Tcp.Abort
      case "halfclose" => Tcp.ConfirmedClose
      case "close" | _ => Tcp.Close
    }

    override def produceFatalError(tcpConnection: ActorRef) = tcpConnection ! closeCommand
  }
}