package sample.reactivekafka

import java.util.{ Properties, UUID }

import akka.actor.{ ActorSystem, Props }
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.scaladsl.{ Sink, Source }
import com.softwaremill.react.kafka.{ ConsumerProperties, ProducerProperties, ReactiveKafka }
import utils.embeddedkafka.KafkaLocal
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Application extends App {

  implicit val system = ActorSystem("CurrencyWatcher")
  val coordinator = system.actorOf(Props(new Coordinator()))
  coordinator ! "Start"
}
