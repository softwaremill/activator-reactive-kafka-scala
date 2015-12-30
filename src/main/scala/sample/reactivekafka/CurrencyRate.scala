package sample.reactivekafka

import java.util

import org.apache.kafka.common.serialization.{ Serializer, Deserializer }

case class CurrencyRateUpdated(base: String, counter: String, percentUpdate: BigDecimal) {
  def asKeyValue = (base, counter) -> percentUpdate
}

object CurrencyRateUpdatedSerializer extends Serializer[CurrencyRateUpdated] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def serialize(s: String, r: CurrencyRateUpdated): Array[Byte] =
    s"${r.base}/${r.counter}/${r.percentUpdate}".getBytes("UTF-8")

  override def close(): Unit = ()
}

object CurrencyRateUpdatedDeserializer extends Deserializer[CurrencyRateUpdated] {

  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def close(): Unit = ()

  override def deserialize(s: String, bytes: Array[Byte]): CurrencyRateUpdated = {
    val Array(base, counter, updateStr) = new String(bytes, "UTF-8").split('/')
    CurrencyRateUpdated(base, counter, BigDecimal(updateStr))
  }
}