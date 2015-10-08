package sample.reactivekafka

import spray.json.DefaultJsonProtocol

case class CurrencyRateUpdated(base: String, counter: String, percentUpdate: BigDecimal) {
  def asKeyValue = (base, counter) -> percentUpdate
}

object CurrencyRateProtocols extends DefaultJsonProtocol {
  implicit val currencyRteFormat = jsonFormat3(CurrencyRateUpdated)
}

import CurrencyRateProtocols._

object CurrencyRateUpdatedEncoder extends kafka.serializer.Encoder[CurrencyRateUpdated] {
  import spray.json._
  override def toBytes(r: CurrencyRateUpdated): Array[Byte] = r.toJson.compactPrint.getBytes("UTF-8")
}

object CurrencyRateUpdatedDecoder extends kafka.serializer.Decoder[CurrencyRateUpdated] {
  override def fromBytes(bytes: Array[Byte]): CurrencyRateUpdated = {
    import spray.json._
    new String(bytes, "UTF-8").parseJson.convertTo[CurrencyRateUpdated]
  }
}