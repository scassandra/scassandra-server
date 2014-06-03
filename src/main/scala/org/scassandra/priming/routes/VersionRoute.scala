package org.scassandra.priming.routes

import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging

trait VersionRoute extends HttpService with Logging {

  import org.scassandra.priming.PrimingJsonImplicits._

  val versionRoute =
    path("version") {
      get {
        complete {
          val version = if (getClass.getPackage.getImplementationVersion == null) {
            "unknown"
          } else {
            getClass.getPackage.getImplementationVersion
          }
          Version(version)
        }
      }
    }
}

case class Version(version: String)
