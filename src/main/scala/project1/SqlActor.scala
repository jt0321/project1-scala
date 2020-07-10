package project1

import scalikejdbc._
import scalikejdbc.config._

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors

import scala.collection.immutable
import scala.collection.mutable.LinkedHashMap
import scala.concurrent.Await
import scala.concurrent.Future

import SparkActor.Result

object SqlActor {
  sealed trait Command
  case class SaveToDb(replyTo: ActorRef[String], results: Set[Result]) extends Command

  //val db = Database.forConfig("h2mem1")

  def apply(): Behavior[Command] =
    Behaviors.setup { context =>
      new SqlActor(context).operations(Set.empty[Result])
    }
}

class SqlActor(context: ActorContext[SqlActor.Command]) {
  import SqlActor._
  implicit val ec = context.system.executionContext

  DBs.setupAll()
  implicit val session = AutoSession
  context.system.log.info("DB started!")

  def operations(results: Set[Result]): Behavior[Command] =
    Behaviors.receiveMessage {
      case SaveToDb(replyTo, results) =>
        context.system.log.info("Save command received")
        val sqlResults = writeAllTables(results)
        replyTo ! sqlResults
        operations(results)
    }

  def writeTable(category: String, counts: LinkedHashMap[String, Long]) = {
    val catName = category.replace(" ", "_")
    val catName_category = catName+"_category"
    val catName_count = catName+"_count"
    val catName_table = sqls"${catName}"
    val catName_column1 = sqls"${catName_category}"
    val catName_column2 = sqls"${catName_count}"
    context.system.log.info(catName_table)
    context.system.log.info(catName_column1)
    context.system.log.info(catName_column2)
    val createTable = sql"""CREATE TABLE ${catName_table} (
      ${catName_column1} VARCHAR NOT NULL,
      ${catName_column2} BIGINT)
      """

    val params: Seq[Seq[Any]] = counts.map { case (str: String, long: Long) => Seq(str, long) } toSeq
    val insertCounts = sql"""insert into {$catName} values (?, ?)""".batch(params: _*)
    context.system.log.info(insertCounts.toString())
    (createTable, insertCounts)
  }

  def writeAllTables(results: Set[Result]): String = {
    val writeCats = results.map(_.categories)
    val writeTables = results.map(result => writeTable(result.categories, result.counts)).toSeq

    val writeCounts = DB localTx { implicit session =>
      writeTables map {
        case (createTable, insertCounts) =>
          createTable.execute.apply()
          insertCounts.apply().sum
      }
    }
    writeCats
      .zip(writeCounts)
      .map { case (category, count) => s"${count} records written for ${category}!" }
      .mkString("\n")
  }

}
