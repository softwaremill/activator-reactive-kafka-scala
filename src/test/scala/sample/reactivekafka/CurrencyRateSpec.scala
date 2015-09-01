package sample.reactivekafka

import org.scalatest.{FlatSpec, Matchers}

class CurrencyRateSpec extends FlatSpec with Matchers {

  behavior of "currency rate en/decoder"

  it should "correctly transform object" in {
    // given
    val initialRate = CurrencyRate("EUR", "USD", BigDecimal.valueOf(456.789))

    // when
    val bytes = CurrencyRateEncoder.toBytes(initialRate)
    val resultRate = CurrencyRateDecoder.fromBytes(bytes)

    // then
    resultRate should equal(initialRate)
  }

}
