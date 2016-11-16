package reactivekafka

import javax.inject.{ Inject, Singleton }

import akka.actor.{ ActorSystem, Props }
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import play.api.inject.ApplicationLifecycle
import scala.concurrent.Future

@Singleton
class DemoLifecycle @Inject() (lifecycle: ApplicationLifecycle, system: ActorSystem) extends LazyLogging {

  logger.info("Starting embedded Kafka")
  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2181)
  EmbeddedKafka.start()
  logger.info("Embedded Kafka ready")
  implicit val materializer = ActorMaterializer.create(system)
  val writer = system.actorOf(Props(new RandomNumberWriter))
  Thread.sleep(2000) // for production systems topic auto creation should be disabled
  val loggingConsumer = system.actorOf(Props(new LoggingConsumer))

  lifecycle.addStopHook { () =>
    logger.info("Shutting down application...")
    writer ! RandomNumberWriter.Stop
    loggingConsumer ! LoggingConsumer.Stop
    Future.successful(EmbeddedKafka.stop())
  }
}
