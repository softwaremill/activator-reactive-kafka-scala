package sample.reactivekafka

import akka.actor.{ Actor, ActorLogging }
import akka.stream.Supervision.Resume
import akka.stream.{ Supervision, ActorAttributes, Materializer }
import akka.stream.scaladsl.Source
import com.softwaremill.react.kafka.{ ConsumerProperties, PublisherWithCommitSink, ReactiveKafka }
import kafka.message.MessageAndMetadata

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.math.BigDecimal.RoundingMode

class KafkaReaderCoordinator(mat: Materializer, topicName: String) extends Actor with ActorLogging {

  implicit val materializer = mat
  var consumerWithOffsetSink: PublisherWithCommitSink[CurrencyRate] = _

  var rates: Map[(String, String), BigDecimal] = Map.empty

  override def preStart(): Unit = {
    super.preStart()
    self ! "Init"
  }

  val processingDecider: Supervision.Decider = {
    case e: Exception => log.error(e, "Error when processing exchange rates"); Resume
  }

  override def receive: Receive = {
    case "Init" => initReader()
  }

  def initReader(): Unit = {
    implicit val actorSystem = context.system
    consumerWithOffsetSink = new ReactiveKafka().consumeWithOffsetSink(ConsumerProperties(
      brokerList = "localhost:9092",
      zooKeeperHost = "localhost:2181",
      topic = topicName,
      "group",
      CurrencyRateDecoder
    )
      .kafkaOffsetsStorage()
      .commitInterval(1200 milliseconds)
    )
    log.debug("Starting the reader")
    Source(consumerWithOffsetSink.publisher)
      .map(processMessage)
      .withAttributes(ActorAttributes.supervisionStrategy(processingDecider))
      .to(consumerWithOffsetSink.offsetCommitSink).run()
    context.parent ! "Reader initialized"
  }

  def processMessage(msg: MessageAndMetadata[Array[Byte], CurrencyRate]) = {
    val pairAndRate = msg.message()
    val lastRateOpt = rates.get((pairAndRate.base, pairAndRate.counter))
    lastRateOpt.foreach { lastRate =>
      if (alertTriggered(lastRate, pairAndRate.rate))
        log.info(s"Exchange rate for ${pairAndRate.base}/${pairAndRate.counter} changed by ${percentChange(lastRate, pairAndRate.rate)}%!")
    }
    rates = rates + pairAndRate.asKeyValue
    msg
  }

  def alertTriggered(lastRate: BigDecimal, rate: BigDecimal): Boolean = percentChange(lastRate, rate).abs > 60

  def percentChange(lastRate: BigDecimal, rate: BigDecimal) = {
    ((rate - lastRate) * 100.0 / rate).setScale(3, RoundingMode.UP)
  }

  override def postStop(): Unit = {
    consumerWithOffsetSink.cancel()
    super.postStop()
  }
}
