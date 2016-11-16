package reactivekafka

import akka.actor.{ Actor, ActorLogging, Cancellable }
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, Source }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ Duration, _ }
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

      val (control, future) = tickSource
        .map(new ProducerRecord[Array[Byte], String](RandomNumbers.Topic, _))
        .toMat(kafkaSink)(Keep.both)
        .run()
      future.onFailure {
        case ex =>
          log.error("Stream failed due to error, restarting", ex)
          throw ex
      }
      context.become(running(control))
      log.info(s"Writer now running, writing random numbers to topic ${RandomNumbers.Topic}")
  }

  def running(control: Cancellable): Receive = {
    case Stop =>
      log.info("Stopping Kafka producer stream and actor")
      control.cancel()
      context.stop(self)
  }
}

object RandomNumberWriter {
  case object Run
  case object Stop
}