package org.scassandra.server.priming.batch

import org.scassandra.server.cqlmessages.{Consistency, BatchQueryKind}
import org.scassandra.server.priming.query.Then

case class BatchPrimeSingle(when: BatchWhen, thenDo: Then)

case class BatchWhen(queries: List[BatchQueryPrime], consistency: Option[List[Consistency]] = None)

case class BatchQueryPrime(text: String, kind: BatchQueryKind)
