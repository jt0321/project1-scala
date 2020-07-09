package project1

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.stream.Materializer

import scala.concurrent.Await
import scala.concurrent.Future

/** Entry point to route Spark job requests
  *
  * @param csvParser
  * @param mat
  */
class SparkRoutes(baseUri: String, sparkActor: ActorRef[SparkActor.Command], sqlActor: ActorRef[SqlActor.Command])(
    implicit
    system: ActorSystem[_],
    mat: Materializer
) {
  import SparkActor._

  implicit val timeout = akka.util.Timeout.create(java.time.Duration.ofDays(30))
  //val timeOut = scala.concurrent.duration.Duration("30 s")
  implicit val ec = system.executionContext
  //implicit val scheduler = system.scheduler

  def getAllCategories(): Future[String] =
    sparkActor.ask(GetAllCategories)
  def calculateCounts(cats: Seq[String]): Future[String] =
    sparkActor.ask(CalculateCounts(_, cats))
  def getCalculated(): Future[String] =
    sparkActor.ask(GetCalculated)
  def saveCounts(sqlActor: ActorRef[SqlActor.Command]): Future[ActionPerformed] =
    sparkActor.ask(SaveCounts(_, sqlActor))

  /** Http handler for spark job
    *
    */
  val sparkRoutes: HttpRequest => HttpResponse = {
    // GET categories
    case HttpRequest(GET, uri, _, _, _) =>
      uri.path match {
        case Uri.Path(s) if s == s"${baseUri}/calculated" =>
          /* val future = getCalculated
          val result = Await.result(future, timeOut).asInstanceOf[String]
          HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"$result")) */
          sparkResponse(getCalculated)
        case Uri.Path(baseUri) =>
          sparkResponse(getAllCategories)
        case _ => HttpResponse(404, entity = "GET failed: Unknown resource! \n")
      }

    // POST methods
    case HttpRequest(POST, uri, _, _, _) =>
      if (uri.query().isEmpty || !uri.path.toString.equals(baseUri))
        HttpResponse(404, entity = "POST failed: Unknown resource! \n")

      // save option
      if (uri.query().getOrElse("save", "false") == "true") {
        // println("Sending results to be saved... oops, not implemented")
        system.log.info("Asking saveCounts")
        val future = saveCounts(sqlActor)
        val result = Await.result(future, scala.concurrent.duration.Duration.Inf).asInstanceOf[ActionPerformed]
        HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"$result"))
      }
      // spark jobs
      uri.query().getAll("cat") match {
        case List(cat1, cat2) =>
          val future = calculateCounts(Seq(cat1, cat2))
          val result = Await.result(future, scala.concurrent.duration.Duration.Inf).asInstanceOf[String]
          HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"$result"))
        case List(cat1) =>
          val future = calculateCounts(Seq(cat1))
          val result = Await.result(future, scala.concurrent.duration.Duration.Inf).asInstanceOf[String]
          HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"$result"))
        case Nil =>
          HttpResponse(404, entity = "No counts requested! \n")
      }

    // important to drain incoming HTTP Entity stream
    case r: HttpRequest =>
      r.discardEntityBytes()
      HttpResponse(404, entity = "Unknown method/resource! \n")
  }

  /** Generic http transform for Spark responses
    *
    * @param ask
    * @return
    */
  def sparkResponse(ask: () => Future[String]): HttpResponse = {
    val future = ask()
    val result = Await.result(future, scala.concurrent.duration.Duration.Inf).asInstanceOf[String]
    HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"$result"))
  }

  /** What core API methods actually do
    *
    */
  val testing: HttpRequest => HttpResponse = { // when tested with `curl "http://localhost:8080/spark?cat=Primary%20Type&cat=Location%20Description"`
    case HttpRequest(POST, uri, _, _, _) =>
      println("uri: " + uri) // http://localhost:8080/spark?cat=Primary%20Type&cat=Location%20Description
      println("uri.path: " + uri.path) // /spark
      println("uri.query(): " + uri.query()) // cat=Primary+Type&cat=Location+Description
      println("uri.queryString(): " + uri.queryString()) // Some(cat=Primary Type&cat=Location Description)
      println("Uri.Query.Empty: " + Uri.Query.Empty) //
      println("Uri.Query(\"\"): " + Uri.Query("")) //
      println("Uri.Query(\"\").equals(Uri.Query.Empty): " + Uri.Query("").equals(Uri.Query.Empty)) // false
      println(
        "Uri.Query.Cons(\"cat\", \"value\", uri.query()) " +
          Uri.Query.Cons("cat", "value", uri.query())
      ) // cat=value&cat=Primary+Type&cat=Location+Description
      println("uri.query().getAll(\"cat\"): " + uri.query().getAll("cat")) // List(Location Description, Primary Type)
      HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, s"watttttt\n"))
  }

}
