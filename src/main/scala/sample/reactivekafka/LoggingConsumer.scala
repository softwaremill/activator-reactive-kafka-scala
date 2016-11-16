package sample.reactivekafka

import akka.actor.{ Actor, ActorLogging }
import akka.kafka.ConsumerMessage.{ CommittableMessage, CommittableOffsetBatch }
import akka.stream.Materializer
import akka.stream.scaladsl.Sink

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class LoggingConsumer(implicit mat: Materializer) extends Actor with ActorLogging {
  import LoggingConsumer._

  override def preStart(): Unit = {
    super.preStart()
    self ! Start
  }

  override def receive: Receive = {
    case Start =>
      log.info("Initializing logging consumer")
      val source = RandomNumberSource.create("loggingConsumer")(context.system)
      source
        .mapAsync(2)(processMessage)
        .map(_.committableOffset)
        .groupedWithin(10, 15 seconds)
        .map(group => group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem) })
        .mapAsync(3)(_.commitScaladsl())
        .runWith(Sink.ignore)
      log.info("Logging consumer started")
  }

  private def processMessage(msg: Message): Future[Message] = {
    log.info(s"Consumed number: ${msg.record.value()}")
    Future.successful(msg)
  }
}

object LoggingConsumer {
  type Message = CommittableMessage[Array[Byte], String]
  case object Start
}
