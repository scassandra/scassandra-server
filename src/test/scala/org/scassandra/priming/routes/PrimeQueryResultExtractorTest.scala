package org.scassandra.priming.routes

import java.util.concurrent.TimeUnit

import org.scalatest.{Matchers, FunSuite}
import org.scassandra.priming.query.{Prime, Then, When, PrimeQuerySingle}

import scala.concurrent.duration.FiniteDuration

/**
 * PrimeQueryResultExtractor was "extracted" and thus is primarily tested
 * via the PrimeQueryRoute.
 *
 * Starting to add direct tests as the PrimingQueryRoute test is getting large
 */
class PrimeQueryResultExtractorTest extends FunSuite with Matchers {

  test("Should default fixedDelay to None") {
    val when = When()
    val then = Then(None, None, None, fixedDelay = None)
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, then)

    val primeResult: Prime = PrimeQueryResultExtractor.extractPrimeResult(primeRequest)

    primeResult.fixedDelay should equal(None)
  }

  test("Should record fixedDelay if present") {
    val when = When()
    val fixedDelay: Some[Long] = Some(500)
    val then = Then(None, None, None, fixedDelay)
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, then)

    val primeResult: Prime = PrimeQueryResultExtractor.extractPrimeResult(primeRequest)

    primeResult.fixedDelay should equal(Some(FiniteDuration(500, TimeUnit.MILLISECONDS)))
  }
}
