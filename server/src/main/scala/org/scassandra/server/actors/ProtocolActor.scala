/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.server.actors

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem }
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec._
import org.scassandra.codec.datatype.DataType
import org.scassandra.server.actors.priming.PrimeQueryStoreActor.{ Fatal, Prime, Reply }

trait ProtocolActor extends Actor with ActorLogging {

  def write(message: Message, requestHeader: FrameHeader, target: ActorRef): Unit = {
    target ! ProtocolResponse(requestHeader, message)
  }

  def extractQueryVariables(
    queryText: String,
    queryValues: Option[List[Value]],
    variableTypes: List[DataType])(implicit protocolVersion: ProtocolVersion): Option[List[Any]] = {
    queryValues.flatMap { (values: List[Value]) =>
      if (values.length == variableTypes.length) {
        Some(values.zip(variableTypes).map {
          case (Bytes(bytes), dataType) => dataType.codec.decodeValue(bytes.toBitVector).require // TODO: handle decode failure
          case (_, dataType) => null // TODO: Handle Null and Unset case.
        })
      } else {
        log.warning(s"Mismatch of variables between query $queryText with variable count ${values.length}, but expected ${variableTypes.length} ($variableTypes).")
        None
      }
    }
  }
}

object ProtocolActor {
  def writePrime(input: Message, primeOption: Option[Prime],
    requestHeader: FrameHeader,
    target: ActorRef, alternative: Option[Prime] = None,
    consistency: Option[Consistency] = None)(implicit system: ActorSystem): Unit = {
    primeOption match {
      case Some(prime) =>
        prime match {
          case Reply(message, _, _) =>
            val msg = ProtocolResponse(requestHeader, messageWithConsistency(message, consistency.getOrElse(Consistency.ONE)))
            prime.fixedDelay match {
              case None => target ! msg
              case Some(duration) => system.scheduler.scheduleOnce(duration, target, msg)(system.dispatcher)
            }
          case f: Fatal =>
            prime.fixedDelay match {
              case None => f.produceFatalError(target)
              case Some(duration) => system.scheduler.scheduleOnce(duration) {
                f.produceFatalError(target)
              }(system.dispatcher)
            }
        }
      case None =>
        alternative.foreach(_ => writePrime(input, alternative, requestHeader, target))
    }
  }

  /**
   * Update the message with the given consistency if the message is an error containing consistency and the consistency
   * wasn't provided by the prime.
   *
   * @param input       message to update.
   * @param consistency consistency to update error message with.
   * @return Updated message if applicable.
   */
  private[this] def messageWithConsistency(input: Message, consistency: Consistency): Message = input match {
    case u @ Unavailable(_, null, _, _) => u.copy(consistency = consistency)
    case w @ WriteTimeout(_, null, _, _, _) => w.copy(consistency = consistency)
    case r @ ReadTimeout(_, null, _, _, _) => r.copy(consistency = consistency)
    case r @ ReadFailure(_, null, _, _, _, __) => r.copy(consistency = consistency)
    case w @ WriteFailure(_, null, _, _, _, __) => w.copy(consistency = consistency)
    case _ => input
  }
}

case class ProtocolMessage(frame: Frame)

case class ProtocolResponse(requestHeader: FrameHeader, response: Message)