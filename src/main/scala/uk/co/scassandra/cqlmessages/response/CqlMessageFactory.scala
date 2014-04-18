package uk.co.scassandra.cqlmessages.response

import org.scassandra.cqlmessages.response._
import org.scassandra.cqlmessages.response.QueryBeforeReadyMessage
import org.scassandra.cqlmessages.response.Rows
import org.scassandra.cqlmessages.response.UnavailableException
import org.scassandra.cqlmessages.response.Ready
import uk.co.scassandra.priming.Prime
import org.scassandra.cqlmessages.response.ReadRequestTimeout
import org.scassandra.cqlmessages.response.SetKeyspace
import org.scassandra.cqlmessages.response.WriteRequestTimeout

trait CqlMessageFactory {
  def createReadyMessage(stream : Byte) : Ready
  def createQueryBeforeErrorMessage() : QueryBeforeReadyMessage
  def createSetKeyspaceMessage(keyspaceName: String, stream: Byte): SetKeyspace
  def createRowsMessage(prime: Prime, stream: Byte): Rows
  def createReadTimeoutMessage(stream: Byte): ReadRequestTimeout
  def createWriteTimeoutMessage(stream: Byte): WriteRequestTimeout
  def createUnavailableMessage(stream: Byte): UnavailableException
  def createVoidMessage(stream: Byte): VoidResult
}
