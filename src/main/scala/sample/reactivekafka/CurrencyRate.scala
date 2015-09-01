package sample.reactivekafka

case class CurrencyRate(base: String, counter: String, rate: BigDecimal) {
  def asKeyValue = (base, counter) -> rate
}

object CurrencyRateEncoder extends kafka.serializer.Encoder[CurrencyRate] {
  override def toBytes(r: CurrencyRate): Array[Byte] = s"${r.base}/${r.counter}/${r.rate}".getBytes("UTF-8")
}

object CurrencyRateDecoder extends kafka.serializer.Decoder[CurrencyRate] {
  override def fromBytes(bytes: Array[Byte]): CurrencyRate = {
    val Array(base, counter, rateStr) = new String(bytes, "UTF-8").split('/')
    CurrencyRate(base, counter, BigDecimal(rateStr))
  }
}