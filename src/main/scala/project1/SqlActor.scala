package project1

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import slick.jdbc.H2Profile.api._

import scala.collection.immutable
import scala.collection.mutable.LinkedHashMap
import scala.concurrent.Await
import scala.concurrent.Future
import scala.collection.mutable

import SparkActor.Result

object SqlActor {
  sealed trait Command
  case class SaveToDb(replyTo: ActorRef[String], results: Set[Result]) extends Command

  val db = Database.forConfig("h2mem1")

  def apply(): Behavior[Command] =
    Behaviors.setup { context =>
      new SqlActor(context).operations(Set.empty[Result])
    }
}

class SqlActor(context: ActorContext[SqlActor.Command]) {
  import SqlActor._
  implicit val ec = context.system.executionContext

  context.system.log.info("DB STARTED!")
  context.system.log.info(db.toString())

  def operations(results: Set[Result]): Behavior[Command] =
    Behaviors.receiveMessage {
      case SaveToDb(replyTo, results) =>
        context.system.log.info("Save command received")
        val sqlResults = writeAllTables(results)
        replyTo ! sqlResults
        operations(results)
    }

  def writeTable(
      category: String,
      counts: LinkedHashMap[String, Long]
  ): DBIO[Unit] = {
    // (String, DBIOAction[Int, NoStream, Effect.All]) = {
    context.system.log.info("Creating table for " + category)
    def createTable(category: String): DBIO[Int] = sqlu"""CREATE TABLE ${category} (
      ${category}_category VARCHAR NOT NULL,
      ${category}_count BIGINT)
      """
    def insert(table: String, c: (String, Long)): DBIO[Int] =
      sqlu"INSERT INTO ${table} VALUES ('${c._1}', ${c._2})"

    def insertValues(table: String, counts: LinkedHashMap[String, Long]): DBIO[Int] = {
      val inserts: Seq[DBIO[Int]] = counts.map(insert(table, _)).toSeq
      val combined: DBIO[Seq[Int]] = DBIO.sequence(inserts)
      combined.map(_.sum)
    }

    // return numer of rows written
    //(category, createTable.zipWith(insertValues)((a, b) => b))
    val catName = category.replace(" ", "_")

    context.system.log.info(createTable(catName).toString())
    context.system.log.info(insertValues(catName, counts).toString())
    DBIO.seq(createTable(catName), insertValues(catName, counts))
  }

  def writeAllTables(results: Set[Result]): String = {
    try {
      context.system.log.info("Some crazy shit")
      /*val allTables = results.map(result => writeTable(result.categories, result.counts)).toSeq
      context.system.log.info(allTables.mkString(", "))
      val allCats = allTables.map(_._1)
      context.system.log.info(allCats.mkString(", "))
      // val allQueries = DBIO.sequence(allTables.map(_._2))
      val allQueries: DBIO[Unit] = DBIO.seq(allTables.map(_._2): _*)
      context.system.log.info(allQueries.toString())
      // DBIO.fold[Int, Effect.All](allTables.map(_._2), 0)((a, b) => Seq(a) :+ Seq(b))
       */
      val allQueries = writeTable(results.head.categories, results.head.counts)
      val f: Future[_] = {
        // val allQueries: DBIO[Int] = DBIO.seq(allTables: _*)
        db.run(allQueries)
      }
      context.system.log.info("Are we still here?")
      //val sqlR =
      Await.result(f, scala.concurrent.duration.Duration.Inf)
      // val rMaps = allCats.zip(sqlR)
      // val resultStr = rMaps.map { case (table, inserted) => s"Inserted ${inserted} rows into table ${table}" }.mkString("\n")
      val resultStr = "Insertion completed!"
      context.system.log.info(resultStr)
      resultStr
    } finally db.close
  }
}
