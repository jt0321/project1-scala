package project1

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors

import scala.collection.immutable
import scala.collection.mutable.LinkedHashMap
import scala.concurrent.Await
import scala.concurrent.Future

object SparkActor {
  sealed trait Command // get categories, get count, get counts, save to db
  final case class GetAllCategories(replyTo: ActorRef[String]) extends Command
  final case class CalculateCounts(replyTo: ActorRef[String], cats: Seq[String]) extends Command
  final case class GetCalculated(replyTo: ActorRef[String]) extends Command
  final case class SaveCounts(replyTo: ActorRef[ActionPerformed], sqlActor: ActorRef[SqlActor.Command]) extends Command

  final case class Count(catValue: String, countValue: Long)
  final case class Result(categories: String, counts: LinkedHashMap[String, Long])
  final case class Results(results: immutable.Seq[Result])

  final case class ActionPerformed(description: String)

  def apply()(implicit csvParser: CsvParser): Behavior[Command] =
    Behaviors.setup { context =>
      new SparkActor(csvParser, context).tasks(Set.empty[Result])
    }
}

class SparkActor(
    csvParser: CsvParser,
    context: ActorContext[SparkActor.Command]
) {
  import SparkActor._
  import SqlActor.SaveToDb

  implicit val timeout = akka.util.Timeout.create(java.time.Duration.ofDays(30))
  val timeOut = scala.concurrent.duration.Duration.Inf
  implicit val ec = context.system.executionContext
  implicit val scheduler = context.system.scheduler

  val allCategories = s"Aspects of Crimes! ${csvParser.header.mkString(", ")}\n"

  private def tasks(results: Set[Result]): Behavior[Command] =
    Behaviors.receiveMessage {
      case GetAllCategories(replyTo) =>
        replyTo ! allCategories
        Behaviors.same
      case CalculateCounts(replyTo, cats) =>
        val catsSorted = cats.sorted.mkString(" ")
        val result = results.find(_.categories == catsSorted) match {
          case Some(i) => i
          case None => Result(catsSorted, csvParser.getCounts(cats))
        }
        if (result.counts.isEmpty) {
          replyTo ! "Invalid category"
          Behaviors.same
        } else {
          val countInstance = CsvParser.Count(Option(result.counts))
          replyTo ! cats.sorted.mkString(" x ") + "\n" + CsvParser.asString(countInstance) + "\n"
          tasks(results + result)
        }
      case GetCalculated(replyTo) =>
        replyTo ! "Performed counts for " + results.map(_.categories).mkString(", ") + "\n"
        Behaviors.same
      case SaveCounts(replyTo, sqlActor) =>
        val sqlFuture: Future[String] = sqlActor.ask(SaveToDb(_, results))
        val sqlResult = Await.result(sqlFuture, timeOut).asInstanceOf[String]
        replyTo ! ActionPerformed(sqlResult)
        Behaviors.same
    }
}
