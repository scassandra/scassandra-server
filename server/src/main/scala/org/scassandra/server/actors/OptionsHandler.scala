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

import akka.actor.{ActorRef, Actor}
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.cqlmessages.CqlMessageFactory

class OptionsHandler(connection: ActorRef, msgFactory: CqlMessageFactory) extends Actor with LazyLogging {
  import org.scassandra.server.actors.OptionsHandlerMessages._

  override def receive: Receive = {
    case msg @ OptionsMessage(stream) => {
      logger.debug(s"Received OPTIONS message $msg")
      connection ! msgFactory.createSupportedMessage(stream)
    }
    case msg @ _ => {
      logger.debug(s"Received unknown message $msg")
    }
  }
}

object OptionsHandlerMessages {
  case class OptionsMessage(stream: Byte)
}
