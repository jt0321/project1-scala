package project1

import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.stream.Materializer
import scala.concurrent.Future

object SparkServer {
  def apply(): Behavior[Nothing] =
    Behaviors.setup[Nothing] { context =>
      new SparkServer(context)
    }
}

class SparkServer(context: ActorContext[Nothing]) extends AbstractBehavior[Nothing](context) {
  implicit val CrimesParser = new CsvParser("Crimes2015.csv")
  val sparkActor = context.spawn(SparkActor(), "SparkActor")
  context.watch(sparkActor)

  //implicit val system = akka.actor.ActorSystem()
  //implicit val executionContext = system.dispatcher
  implicit val system = context.system
  implicit val mat = Materializer(context)
  val routes = new SparkRoutes("/spark", sparkActor)

  sys.addShutdownHook(println("Server terminated!"))
  httpServer(routes, system)

  /** Akka Http is still on classic ActorSystem so needs some stuff for compatibility purposes
    *
    * @param routes
    * @param system
    */
  private def httpServer(routes: SparkRoutes, system: ActorSystem[_]): Unit = {
    // implicit val mat = ActorMaterializer()
    // implicit val executionContext = system.dispatcher

    implicit val classicSystem: akka.actor.ActorSystem = system.toClassic
    implicit val ec = system.executionContext
    val bindingFuture = Http().bindAndHandleSync(routes.sparkRoutes, "localhost", 8080)
    println(s"Server online at http://localhost:8080/\nPress CTRL+C to stop...")
  }

  /** This method makes this class no longer abstract, yay!
    *
    * @param msg
    * @return
    */
  def onMessage(msg: Nothing): Behavior[Nothing] = {
    Behaviors.empty
  }
}

object SimpleServer extends App {
  ActorSystem[Nothing](SparkServer(), "Project1HttpServer")
}
