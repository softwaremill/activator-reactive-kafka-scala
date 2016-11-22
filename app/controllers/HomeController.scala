package controllers

import javax.inject._
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Sink, Source }
import play.api.mvc.{ Action, Controller, WebSocket }
import reactivekafka.RandomNumberSource

import scala.concurrent.Future

@Singleton
class HomeController @Inject() (implicit system: ActorSystem) extends Controller {

  def index = Action { implicit request =>
    Ok(views.html.index(routes.HomeController.ws().webSocketURL()))
  }

  def ws = WebSocket.acceptOrResult[Any, String] { _ =>
    val source = RandomNumberSource.create("websocketConsumer")
    val wsFlow = Flow.fromSinkAndSource(Sink.ignore, source.map(_.record.value))
    Future.successful(Right(wsFlow))
  }

}