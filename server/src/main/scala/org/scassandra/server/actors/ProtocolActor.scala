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

import akka.actor.{Actor, ActorLogging, ActorRef}
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec._
import org.scassandra.codec.datatype.DataType
import org.scassandra.server.priming.query.{Fatal, Prime, Reply}

trait ProtocolActor extends Actor with ActorLogging {

  def write(message: Message, requestHeader: FrameHeader, recipient: Option[ActorRef] = None) = {
    val target = recipient.getOrElse(sender)
    target ! ProtocolResponse(requestHeader, message)
  }

  /**
    * Update the message with the given consistency if the message is an error containing consistency.
    * @param input message to update.
    * @param consistency consistency to update error message with.
    * @return Updated message if applicable.
    */
  private [this] def messageWithConsistency(input: Message, consistency: Option[Consistency]): Message = consistency match {
    case Some(c) =>
      input match {
        case u @ Unavailable(_, uc, _ , _) if c != uc => u.copy(consistency = c)
        case w @ WriteTimeout(_, wc, _, _, _) if c != wc => w.copy(consistency = c)
        case r @ ReadTimeout(_, rc, _, _, _) if c != rc => r.copy(consistency = c)
        case r @ ReadFailure(_, rc, _, _, _, __) if c != rc => r.copy(consistency = c)
        case w @ WriteFailure(_, wc, _, _, _, __) if c != wc => w.copy(consistency = c)
        case _ => input
      }
    case None => input
  }

  def writePrime(input: Message, primeOption: Option[Prime], requestHeader: FrameHeader, recipient: Option[ActorRef] = None, alternative: Option[Prime] = None, consistency: Option[Consistency] = None): Unit = {
    val target = recipient.getOrElse(sender)
    primeOption match {
      case Some(prime) =>
        prime match {
          case Reply(message, _, _) =>
            val msg = ProtocolResponse(requestHeader, messageWithConsistency(message, consistency))
            prime.fixedDelay match {
              case None => target ! msg
              case Some(duration) => context.system.scheduler.scheduleOnce(duration, target, msg)(context.system.dispatcher)
            }
          case f: Fatal =>
            prime.fixedDelay match {
              case None => f.produceFatalError(target)
              case Some(duration) => context.system.scheduler.scheduleOnce(duration){ f.produceFatalError(target) }(context.system.dispatcher)
            }
        }
      case None =>
        log.info(s"No prime found for $input")
        alternative.foreach(_ => writePrime(input, alternative, requestHeader, recipient))
    }
  }

  def extractQueryVariables(queryText: String, queryValues: Option[List[QueryValue]], variableTypes: List[DataType]): Option[List[Any]] = {
    queryValues.flatMap { (values: List[QueryValue]) =>
      if (values.length == variableTypes.length) {
        Some(values.zip(variableTypes).map {
          case ((QueryValue(_, Bytes(bytes)), dataType)) => dataType.codec.decodeValue(bytes.toBitVector).require // TODO: handle decode failure
          case ((QueryValue(_, _), dataType)) => null // TODO: Handle Null and Unset case.
        })
      } else {
        log.warning(s"Mismatch of variables between query $queryText with variable count ${values.length}, but expected ${variableTypes.length} ($variableTypes).")
        None
      }
    }
  }
}

case class ProtocolMessage(frame: Frame)

case class ProtocolResponse(requestHeader: FrameHeader, response: Message)