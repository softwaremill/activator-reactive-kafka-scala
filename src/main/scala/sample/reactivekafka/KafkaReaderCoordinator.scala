package sample.reactivekafka

import akka.actor.{Actor, ActorLogging, Props}
import akka.stream.Supervision.Resume
import akka.stream._
import akka.stream.scaladsl._
import com.softwaremill.react.kafka.{ConsumerProperties, ProducerProperties, PublisherWithCommitSink, ReactiveKafka}
import kafka.message.MessageAndMetadata
import kafka.serializer.StringEncoder

import scala.concurrent.duration._
import scala.language.postfixOps

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
    val currencyRateSource = Source(consumerWithOffsetSink.publisher)
    val currencyBroadcaster = Source(currencyRateSource.runWith(Sink.publisher(true)))

    val alertsFlow = Flow[MessageAndMetadata[Array[Byte], CurrencyRateUpdated]]
      .map(processAlert)
      .filter(alert => alert != None)
      .map(alert => alert.get)

    // stream to write alerts to kafka
    currencyBroadcaster
      .via(alertsFlow)
      .to(Sink.actorSubscriber(createKafkaAlertProducerProps(None)))
      .run()

    // stream to write USD alerts to kafka
    currencyBroadcaster
      .via(alertsFlow)
      .filter(alert => alert.contains("USD"))
      .to(Sink.actorSubscriber(createKafkaAlertProducerProps(Some("USD"))))
      .run()

    context.parent ! "Reader initialized"
  }

  private def createKafkaAlertProducerProps(currency: Option[String]): Props = {
    new ReactiveKafka().producerActorProps(ProducerProperties(
      brokerList = "localhost:9092",
      topic = Array(Some(topicName), Some("alert"), currency).flatten.mkString("-"),
      encoder = new StringEncoder()
    ))
  }

  def processAlert(msg: MessageAndMetadata[Array[Byte], CurrencyRateUpdated]): Option[String] = {
    val pairAndRate = msg.message()
    if (alertTriggered(pairAndRate.percentUpdate))
      Some(s"${pairAndRate.base}/${pairAndRate.counter} changed by ${pairAndRate.percentUpdate}%!")
    else
      None
  }

  def alertTriggered(update: BigDecimal): Boolean = update.abs > 3

  override def postStop(): Unit = {
    consumerWithOffsetSink.cancel()
    super.postStop()
  }
}
