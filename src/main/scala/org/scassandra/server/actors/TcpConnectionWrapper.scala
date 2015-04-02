/*
 * Copyright (C) 2014 Christopher Batey and Dogan Narinc
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

import akka.actor.{ActorLogging, ActorRef, Actor}
import org.scassandra.server.cqlmessages.CqlMessage
import akka.io.Tcp.Write
import com.typesafe.scalalogging.slf4j.Logging

class  TcpConnectionWrapper(tcpConnection : ActorRef) extends Actor with ActorLogging {
  def receive: Actor.Receive = {
    case msg : CqlMessage => {
      tcpConnection ! Write(msg.serialize())
    }
    case _ @ msg => {
      log.error(s"Received unknown msg $msg")
    }
  }
}
