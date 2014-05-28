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
package org.scassandra

import org.scalatest.{Matchers, FunSpecLike}

class ServerStubRunnerTest extends FunSpecLike with Matchers {
  ignore("awaitStartup()") {
    it("should complete successfully when both priming server and tcp server are ready") {
      // given
      val underTest = new ServerStubRunner(8046, 8047)

      // TODO - how to mock internal actors? (i.e primingReadyListener and tcpReadyListener)

      // test will fail if this throws a timeout exception
      underTest.awaitStartup()
    }

  }
}
