package com.softwaremill.embeddedkafka

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.Properties

import akka.actor.Actor
import utils.embeddedkafka.KafkaLocal

class EmbeddedKafka extends Actor {

  var embeddedKafka: Option[KafkaLocal] = None

  override def preStart(): Unit = {
    super.preStart()
    deleteKafkaData()
    embeddedKafka = Some(initEmbeddedKafka())
    context.parent ! "Start"
  }

  override def postStop(): Unit = {
    embeddedKafka.foreach(_.stop())
    super.postStop()
  }

  def initEmbeddedKafka() = {
    val kafkaProperties = new Properties()
    val zkProperties = new Properties()
    kafkaProperties.load(getClass.getResourceAsStream("/kafkalocal.properties"))
    zkProperties.load(getClass.getResourceAsStream("/zklocal.properties"))
    new KafkaLocal(kafkaProperties, zkProperties)
  }

  override def receive: Actor.Receive = {
    case _ =>
  }

  def deleteKafkaData(): Unit = {
    val path = Paths.get("./data")
    Files.walkFileTree(path, new FileVisitor[Path] {
      override def visitFileFailed(file: Path, exc: IOException) = FileVisitResult.CONTINUE

      override def visitFile(file: Path, attrs: BasicFileAttributes) = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes) = FileVisitResult.CONTINUE

      override def postVisitDirectory(dir: Path, exc: IOException) = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })
  }
}
