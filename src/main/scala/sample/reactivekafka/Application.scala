package sample.reactivekafka

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._

import akka.actor.{ ActorSystem, Props }
import com.softwaremill.embeddedkafka.EmbeddedKafka

object Application extends App {

  implicit val system = ActorSystem("CurrencyWatcher")
  system.actorOf(Props(new Coordinator()))
}
