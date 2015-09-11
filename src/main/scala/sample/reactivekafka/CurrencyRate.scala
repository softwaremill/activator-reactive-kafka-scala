package sample.reactivekafka

case class CurrencyRateUpdated(base: String, counter: String, percentUpdate: BigDecimal) {
  def asKeyValue = (base, counter) -> percentUpdate
}

object CurrencyRateUpdatedEncoder extends kafka.serializer.Encoder[CurrencyRateUpdated] {
  override def toBytes(r: CurrencyRateUpdated): Array[Byte] = s"${r.base}/${r.counter}/${r.percentUpdate}".getBytes("UTF-8")
}

object CurrencyRateUpdatedDecoder extends kafka.serializer.Decoder[CurrencyRateUpdated] {
  override def fromBytes(bytes: Array[Byte]): CurrencyRateUpdated = {
    val Array(base, counter, updateStr) = new String(bytes, "UTF-8").split('/')
    CurrencyRateUpdated(base, counter, BigDecimal(updateStr))
  }
}