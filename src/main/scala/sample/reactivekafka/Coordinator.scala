package sample.reactivekafka

import java.util.UUID

import akka.actor._
import akka.stream.ActorMaterializer
import com.softwaremill.embeddedkafka.EmbeddedKafka

import scala.concurrent.duration._
import scala.language.postfixOps

class Coordinator extends Actor with ActorLogging {

  val topicName = UUID.randomUUID().toString
  var currencyWriter: Option[ActorRef] = None
  var currencyReader: Option[ActorRef] = None
  var alertReader: Option[ActorRef] = None
  var usdAlertReader: Option[ActorRef] = None
  val materializer = ActorMaterializer()(context)

  implicit val ec = context.dispatcher

  override def preStart(): Unit = {
    super.preStart()
    context.actorOf(Props(new EmbeddedKafka))
  }

  override def receive: Receive = {
    case "Start" =>
      log.debug("Starting the coordinator")
      currencyWriter = Some(context.actorOf(Props(new KafkaWriterCoordinator(materializer, topicName))))
      currencyReader = Some(context.actorOf(Props(new KafkaReaderCoordinator(materializer, topicName))))
      alertReader = Some(context.actorOf(Props(new KafkaAlertReaderCoordinator(materializer, topicName, null))))
      usdAlertReader = Some(context.actorOf(Props(new KafkaAlertReaderCoordinator(materializer, topicName, "USD"))))
    case "Reader initialized" =>
      context.system.scheduler.scheduleOnce(5 seconds, self, "Stop")
    case "Stop" =>
      log.debug("Stopping the coordinator")
      currencyWriter.map(actor => actor ! "Stop")
      currencyReader.map(actor => context.stop(actor))
      alertReader.map(actor => context.stop(actor))
      usdAlertReader.map(actor => context.stop(actor))
      context.system.scheduler.scheduleOnce(5 seconds, self, "Shutdown")
    case "Shutdown" =>
      log.debug("Shutting down the app")
      context.system.shutdown()
  }
}
