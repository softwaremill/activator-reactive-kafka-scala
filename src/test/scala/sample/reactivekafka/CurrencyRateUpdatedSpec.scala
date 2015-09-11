package sample.reactivekafka

import org.scalatest.{ FlatSpec, Matchers }

class CurrencyRateUpdatedSpec extends FlatSpec with Matchers {

  behavior of "currency rate en/decoder"

  it should "correctly transform object" in {
    // given
    val initialRate = CurrencyRateUpdated("EUR", "USD", BigDecimal.valueOf(3))

    // when
    val bytes = CurrencyRateUpdatedEncoder.toBytes(initialRate)
    val resultRate = CurrencyRateUpdatedDecoder.fromBytes(bytes)

    // then
    resultRate should equal(initialRate)
  }

}
