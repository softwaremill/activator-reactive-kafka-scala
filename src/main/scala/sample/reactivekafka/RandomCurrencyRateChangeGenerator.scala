package sample.reactivekafka

import java.security.SecureRandom

import scala.annotation.tailrec
import scala.math.BigDecimal.RoundingMode

object RandomCurrencyRateChangeGenerator {

  val currencies = List("EUR", "USD", "CHF", "JPY", "AUD")
  val Random = new SecureRandom()

  def randomPair() = {
    val baseCurrency = randomCurrency()
    val counterCurrency = generateCounterCurrency(baseCurrency)
    val percentUpdate = BigDecimal.valueOf(Random.nextDouble() * 5.0).setScale(3, RoundingMode.DOWN)
    CurrencyRateUpdated(baseCurrency, counterCurrency, percentUpdate)
  }

  @tailrec
  private def generateCounterCurrency(base: String): String = {
    val curr = randomCurrency()
    if (curr == base)
      generateCounterCurrency(base)
    else curr
  }

  def randomCurrency() = currencies(Random.nextInt(currencies.size))
}
