package sample.reactivekafka

import akka.actor.{ Terminated, Actor, ActorLogging }
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.softwaremill.react.kafka.{ ConsumerProperties, PublisherWithCommitSink, ReactiveKafka }
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.duration._
import scala.language.postfixOps

class KafkaReaderCoordinator(mat: Materializer, topicName: String) extends Actor with ActorLogging {

  implicit val materializer = mat
  var consumerWithOffsetSink: PublisherWithCommitSink[Array[Byte], CurrencyRateUpdated] = _

  override def preStart(): Unit = {
    super.preStart()
    initReader()
  }

  override def receive: Receive = {
    case Terminated(_) =>
      log.error("The consumer has been terminated, restarting the whole stream")
      initReader()
    case _ =>
  }

  def initReader(): Unit = {
    implicit val actorSystem = context.system
    consumerWithOffsetSink = new ReactiveKafka().consumeWithOffsetSink(ConsumerProperties(
      bootstrapServers = "localhost:9092",
      topic = topicName,
      "group",
      CurrencyRateUpdatedDeserializer
    )
      .commitInterval(1200 milliseconds))
    log.debug("Starting the reader")
    context.watch(consumerWithOffsetSink.publisherActor)
    Source.fromPublisher(consumerWithOffsetSink.publisher)
      .map(processMessage)
      .to(consumerWithOffsetSink.offsetCommitSink).run()
    context.parent ! "Reader initialized"
  }

  def processMessage(msg: ConsumerRecord[Array[Byte], CurrencyRateUpdated]) = {
    val pairAndRate = msg.value()
    if (alertTriggered(pairAndRate.percentUpdate)) {
      saveMessageToDb(pairAndRate)
    }
    msg
  }

  def saveMessageToDb(pairAndRate: CurrencyRateUpdated): Unit = {
    log.info(s"Exchange rate for ${pairAndRate.base}/${pairAndRate.counter} changed by ${pairAndRate.percentUpdate}%!")
  }

  def alertTriggered(update: BigDecimal): Boolean = update.abs > 3

  override def postStop(): Unit = {
    consumerWithOffsetSink.cancel()
    super.postStop()
  }
}
