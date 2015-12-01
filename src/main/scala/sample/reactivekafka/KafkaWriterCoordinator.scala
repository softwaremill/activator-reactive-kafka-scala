package sample.reactivekafka

import akka.actor.SupervisorStrategy.Resume
import akka.actor._
import akka.stream.Materializer
import akka.stream.actor.ActorSubscriberMessage.OnComplete
import akka.stream.actor.{ActorPublisher, ActorSubscriber}
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.{ProducerProperties, ReactiveKafka}

/**
  * Responsible for starting the writing stream.
  */
class KafkaWriterCoordinator(mat: Materializer, topicName: String) extends Actor with ActorLogging {

  implicit lazy val materializer = mat

  var subscriberActor: Option[ActorRef] = None

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception =>
      // here you can handle your failing Kafka writes
      log.error("Write failed!")
      Resume
  }

  override def preStart(): Unit = {
    super.preStart()
    initWriter()
  }

  override def receive: Receive = {
    case "Stop" =>
      log.debug("Stopping the writer coordinator")
      subscriberActor.foreach(actor => actor ! OnComplete)
  }

  def initWriter(): Unit = {
    val actorProps = new ReactiveKafka().producerActorProps(ProducerProperties(
      brokerList = "localhost:9092",
      topic = topicName,
      encoder = CurrencyRateUpdatedEncoder
    ))
    val actor = context.actorOf(actorProps)
    subscriberActor = Some(actor)
    val generatorActor = context.actorOf(Props(new CurrencyRatePublisher))

    // Start the stream
    Source(ActorPublisher(generatorActor))
      .runWith(Sink(ActorSubscriber[CurrencyRateUpdated](actor)))
  }

}
