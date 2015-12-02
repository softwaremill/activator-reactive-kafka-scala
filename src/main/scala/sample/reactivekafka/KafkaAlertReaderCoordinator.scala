package sample.reactivekafka

import akka.actor.{Actor, ActorLogging}
import akka.stream.Supervision.Resume
import akka.stream.scaladsl.Source
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.softwaremill.react.kafka.{ConsumerProperties, PublisherWithCommitSink, ReactiveKafka}
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder

import scala.concurrent.duration._

class KafkaAlertReaderCoordinator(mat: Materializer, topicName: String, currency: Option[String]) extends Actor with ActorLogging {
  implicit val materializer = mat
  var consumerWithOffsetSink: PublisherWithCommitSink[String] = _
  val processingDecider: Supervision.Decider = {
    case e: Exception => log.error(e, "Error when processing alert"); Resume
  }

  override def preStart(): Unit = {
    super.preStart()
    initReader()
  }

  override def postStop(): Unit = {
    consumerWithOffsetSink.cancel()
    super.postStop()
  }

  override def receive: Receive = {
    case _ =>
  }

  private def createKafkaReader(): PublisherWithCommitSink[String] = {
    implicit val actorSystem = context.system

    val consumerProperties = ConsumerProperties(
      brokerList = "localhost:9092",
      zooKeeperHost = "localhost:2181",
      topic = getTopicName(currency),
      "group",
      new StringDecoder()
    )

    new ReactiveKafka().consumeWithOffsetSink(consumerProperties
      .kafkaOffsetsStorage()
      .commitInterval(1200 milliseconds))
  }

  private def initReader(): Unit = {
    implicit val actorSystem = context.system

    log.debug("Starting the kafka alert reader")
    consumerWithOffsetSink = createKafkaReader()
    val alertStream = Source(consumerWithOffsetSink.publisher)

    alertStream
      .map(outputMessage)
      .withAttributes(ActorAttributes.supervisionStrategy(processingDecider))
      .to(consumerWithOffsetSink.offsetCommitSink)
      .run()

    log.debug("KafkaAlertReaderCoordinator initialized")
  }

  def outputMessage(msg: MessageAndMetadata[Array[Byte], String]) = {
    val alert = msg.message()
    log.info(s"topic: ${getTopicName(currency)} - ${alert}")

    msg
  }

  private def getTopicName(currency: Option[String]) = {
    Array(Some(topicName), Some("alert"), currency).flatten.mkString("-")
  }
}
