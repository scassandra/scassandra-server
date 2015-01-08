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
package org.scassandra.server.priming

abstract class Result(val string: String)

case object Success extends Result("success")

case object ReadTimeout extends Result("read_request_timeout")

case object Unavailable extends Result("unavailable")

case object WriteTimeout extends Result("write_request_timeout")

object Result {
  def fromString(string: String): Result = {
    string match {
      case ReadTimeout.string => ReadTimeout
      case Unavailable.string => Unavailable
      case WriteTimeout.string => WriteTimeout
      case Success.string => Success
      case _ => Success
    }
  }
}


