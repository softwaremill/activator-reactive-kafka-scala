package sample.reactivekafka

import akka.actor._
import akka.stream.Materializer
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorSubscriberMessage.OnComplete
import akka.stream.actor.{ ActorPublisher, ActorSubscriber }
import akka.stream.scaladsl.{ Sink, Source }
import com.softwaremill.react.kafka.{ ProducerMessage, ProducerProperties, ReactiveKafka }
import org.reactivestreams.Publisher

/**
 * Responsible for starting the writing stream.
 */
class KafkaWriterCoordinator(mat: Materializer, topicName: String) extends Actor with ActorLogging {

  implicit lazy val materializer = mat

  var subscriberActorOpt: Option[ActorRef] = None
  var generatorActorOpt: Option[ActorRef] = None

  override def preStart(): Unit = {
    super.preStart()
    initWriter()
  }

  override def receive: Receive = {
    case "Stop" =>
      log.debug("Stopping the writer coordinator")
      subscriberActorOpt.foreach(actor => actor ! OnComplete)
      generatorActorOpt.foreach(actor => actor ! Cancel)
    case Terminated(_) =>
      log.error("The producer has been terminated, restarting the whole stream")
      generatorActorOpt.foreach(actor => actor ! Cancel)
      initWriter()

  }

  def initWriter(): Unit = {
    val actorProps = new ReactiveKafka().producerActorProps(ProducerProperties(
      bootstrapServers = "localhost:9092",
      topic = topicName,
      valueSerializer = CurrencyRateUpdatedSerializer
    ))
    val subscriberActor = context.actorOf(actorProps)
    subscriberActorOpt = Some(subscriberActor)
    val generatorActor = context.actorOf(Props(new CurrencyRatePublisher))
    generatorActorOpt = Some(context.actorOf(Props(new CurrencyRatePublisher)))
    context.watch(subscriberActor)

    // Start the stream
    val publisher: Publisher[CurrencyRateUpdated] = ActorPublisher[CurrencyRateUpdated](generatorActor)
    Source.fromPublisher(publisher).map(msg => ProducerMessage(msg))
      .runWith(Sink.fromSubscriber(ActorSubscriber[ProducerMessage[Array[Byte], CurrencyRateUpdated]](subscriberActor)))
  }

}
