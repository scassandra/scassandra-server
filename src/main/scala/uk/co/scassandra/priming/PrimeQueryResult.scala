package uk.co.scassandra.priming

import spray.json.JsArray

case class PrimeQueryResult(when: String, then: JsArray, metadata: Option[Metadata])

case class Metadata(result: Option[String])
