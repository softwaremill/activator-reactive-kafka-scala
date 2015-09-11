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
  var consumerWithOffsetSink: PublisherWithCommitSink[CurrencyRateUpdated] = _

  override def preStart(): Unit = {
    super.preStart()
    initReader()
  }

  val processingDecider: Supervision.Decider = {
    case e: Exception => log.error(e, "Error when processing exchange rates"); Resume
  }

  override def receive: Receive = {
    case _ =>
  }

  def initReader(): Unit = {
    implicit val actorSystem = context.system
    consumerWithOffsetSink = new ReactiveKafka().consumeWithOffsetSink(ConsumerProperties(
      brokerList = "localhost:9092",
      zooKeeperHost = "localhost:2181",
      topic = topicName,
      "group",
      CurrencyRateUpdatedDecoder
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

  def processMessage(msg: MessageAndMetadata[Array[Byte], CurrencyRateUpdated]) = {
    val pairAndRate = msg.message()
    if (alertTriggered(pairAndRate.percentUpdate))
      log.info(s"Exchange rate for ${pairAndRate.base}/${pairAndRate.counter} changed by ${pairAndRate.percentUpdate}%!")
    msg
  }

  def alertTriggered(update: BigDecimal): Boolean = update > 3

  def percentChange(lastRate: BigDecimal, rate: BigDecimal) = {
    ((rate - lastRate) * 100.0 / rate).setScale(3, RoundingMode.UP)
  }

  override def postStop(): Unit = {
    consumerWithOffsetSink.cancel()
    super.postStop()
  }
}
