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
package org.scassandra

import scodec.Attempt.{Failure, Successful}
import scodec.bits.ByteVector
import scodec.{Codec, DecodeResult}

import scala.util.{Try, Failure => TFailure}

package object codec {

  def next[T <: AnyRef](bytes: ByteVector)(implicit c: shapeless.Lazy[Codec[T]]): Try[(T, ByteVector)] = next(Codec[T], bytes)

  def next[T <: AnyRef](codec: Codec[T], bytes: ByteVector): Try[(T, ByteVector)] = {
    codec.decode(bytes.toBitVector) match {
      case Successful(DecodeResult(result, remainder)) => Try((result, remainder.bytes))
      case Failure(x) => TFailure(new Exception(x.toString))
    }
  }
}
