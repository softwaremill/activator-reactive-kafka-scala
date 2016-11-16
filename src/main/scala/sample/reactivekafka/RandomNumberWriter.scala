package sample.reactivekafka

import akka.actor.{ Actor, ActorLogging }
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.{ ActorMaterializer, Materializer }
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }

import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import scala.util.Random

/**
 * Generates random numbers and puts them to Kafka.
 */
class RandomNumberWriter(implicit mat: Materializer) extends Actor with ActorLogging {

  import RandomNumberWriter._

  override def preStart(): Unit = {
    super.preStart()
    self ! Run
  }

  override def receive: Receive = {
    case Run =>
      val tickSource = Source.tick(Duration.Zero, 1.second, Unit).map(_ => Random.nextInt().toString)
      val producerSettings = ProducerSettings(context.system, new ByteArraySerializer, new StringSerializer)
        .withBootstrapServers("localhost:9092")
      log.info("Initializing writer")
      val kafkaSink = Producer.plainSink(producerSettings)
      // todo err handling
      tickSource
        .map(new ProducerRecord[Array[Byte], String](RandomNumbers.Topic, _))
        .to(kafkaSink)
        .run()
      log.info(s"Writer now running, writing random numbers to topic ${RandomNumbers.Topic}")
  }
}

object RandomNumberWriter {
  case object Run
}