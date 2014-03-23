package uk.co.scassandra.priming

import spray.json.JsArray

case class PrimeQueryResult(when: String, then: Then)

case class Then(rows: Option[JsArray], result: Option[String] = None)
