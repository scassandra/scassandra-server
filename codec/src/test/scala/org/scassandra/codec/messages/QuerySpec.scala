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
package org.scassandra.codec.messages

import org.scassandra.codec.{CodecSpec, Consistency, ProtocolVersion}
import scodec.Attempt.Successful

class QuerySpec extends CodecSpec {

  "QueryParameters.codec" when {

    withProtocolVersions { (protocolVersion: ProtocolVersion) =>
      val codec = QueryParameters.codec(protocolVersion)

      val parametersWithPaging = QueryParameters(Consistency.TWO, pageSize = Some(50))

      if(protocolVersion.version == 1) {
        "skip flags even if set" in {
          // we expect flags and their associated values to be truncated for protocol V1.
          val expected = parametersWithPaging.copy(pageSize = None)
          val encoded = codec.encode(parametersWithPaging).require
          val decoded = codec.decodeValue(encoded) should matchPattern {
            case Successful(`expected`) =>
          }
        }
      } else {
        "should include flags" in {
          val encoded = codec.encode(parametersWithPaging).require
          val decoded = codec.decodeValue(encoded) should matchPattern {
            case Successful(`parametersWithPaging`) =>
          }
        }
      }
    }
  }
}
