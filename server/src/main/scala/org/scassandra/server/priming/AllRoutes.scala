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
package org.scassandra.server.priming

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.priming.prepared.PrimePreparedMulti
import org.scassandra.server.priming.routes._

import scala.language.postfixOps

trait AllRoutes extends PrimingPreparedRoute with PrimingQueryRoute with ActivityVerificationRoute with VersionRoute with PrimingBatchRoute with CurrentRoute with PrimingMultiRoute with LazyLogging {

  val allRoutes: Route = routeForPreparedPriming ~
    queryRoute ~ activityVerificationRoute ~
    versionRoute ~ batchRoute ~ currentRoute ~ routeForMulti

}

