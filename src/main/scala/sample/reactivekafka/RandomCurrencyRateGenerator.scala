package sample.reactivekafka

import java.security.SecureRandom

import scala.annotation.tailrec

object RandomCurrencyRateGenerator {

  val currencies = List("EUR", "USD", "CHF", "JPY", "AUD")
  val Random = new SecureRandom()

  def randomPair() = {
    val baseCurrency = randomCurrency()
    val counterCurrency = generateCounterCurrency(baseCurrency)
    val rate = Random.nextDouble()
    CurrencyRate(baseCurrency, counterCurrency, rate)
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
