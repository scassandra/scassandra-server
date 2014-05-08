package uk.co.scassandra.priming

case class PrimePreparedSingle(when: WhenPrepared)

case class WhenPrepared(query: String)
