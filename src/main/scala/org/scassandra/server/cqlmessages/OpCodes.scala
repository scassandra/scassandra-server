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
package org.scassandra.server.cqlmessages

object OpCodes {
  val Register: Byte = 0x0B
  val Error: Byte = 0x00
  val Startup: Byte = 0x01
  val Ready: Byte = 0x02
  val Options: Byte = 0x05
  val Query: Byte = 0x07
  val Result: Byte = 0x08
  val Prepare: Byte = 0x09
  val Execute: Byte = 0x0A
}