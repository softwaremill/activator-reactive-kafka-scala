package sample.reactivekafka

import akka.actor.ActorLogging
import akka.stream.actor.{ ActorPublisher, ActorPublisherMessage }

class CurrencyRatePublisher extends ActorPublisher[CurrencyRate] with ActorLogging {

  override def receive: Receive = {
    case ActorPublisherMessage.Request(_) => sendRates()
    case ActorPublisherMessage.Cancel     => context.stop(self)
    case _                                =>
  }

  def sendRates(): Unit = {
    while (isActive && totalDemand > 0) {
      onNext(RandomCurrencyRateGenerator.randomPair())
      Thread.sleep(300)
    }
  }
}
