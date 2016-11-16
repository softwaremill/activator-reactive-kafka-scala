package sample.reactivekafka

import akka.actor.{ ActorSystem, Props }
import akka.stream.ActorMaterializer
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }

object Application extends App {
  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2181)
  EmbeddedKafka.start()
  implicit val system = ActorSystem("ReactiveKafkaDemo")
  implicit val materializer = ActorMaterializer.create(system)

  system.actorOf(Props(new RandomNumberWriter))
  Thread.sleep(2000) // for production systems topic auto creation should be disabled
  system.actorOf(Props(new LoggingConsumer))

  Thread.sleep(10000)
  EmbeddedKafka.stop()
  system.terminate()
}
