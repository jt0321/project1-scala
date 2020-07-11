package project1

import java.sql._

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.ConfigFactory

import scala.collection.immutable
import scala.collection.mutable.LinkedHashMap
import scala.concurrent.Await
import scala.concurrent.Future

import SparkActor.Result

object SqlActor {
  sealed trait Command
  case class SaveToDb(replyTo: ActorRef[String], results: Set[Result]) extends Command

  object DB {
    def apply(): Connection = {
      val config = ConfigFactory.load("application.conf").getConfig("db")
      val driver = config.getString("driver")
      val url = config.getString("url")
      val user = config.getString("user")
      val password = config.getString("password")

      Class.forName(driver)
      DriverManager.getConnection(url, user, password)
    }
  }

  def apply(): Behavior[Command] =
    Behaviors.setup { context =>
      new SqlActor(context).operations(Set.empty[Result])
    }
}

class SqlActor(context: ActorContext[SqlActor.Command]) {
  import SqlActor._
  implicit val ec = context.system.executionContext

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
    val dropSql = s"DROP TABLE IF EXISTS ${catName}"
    val createSql = s"""CREATE TABLE IF NOT EXISTS ${catName} (
      ${catName}_category VARCHAR,
      ${catName}_count BIGINT)
      """
    val insertSql = s"INSERT INTO {$catName} (${catName}_category, ${catName}_count) VALUES (?,?)"
    (dropSql, createSql, insertSql, counts)
  }

  def writeAllTables(results: Set[Result]): String = {
    try {
      val cats = results.map(_.categories)
      val sql = results.map(result => writeTable(result.categories, result.counts)).toSeq

      val rows = sql.map {
        case (dropSql, createSql, insertSql, counts) =>
          val conn = DB()
          conn.setAutoCommit(false)
          context.system.log.info(s"DB connected! \n${conn.getMetaData()}")

          val create = conn.createStatement
          create.execute(dropSql)
          create.execute(createSql)
          conn.commit

          val insert = conn.prepareStatement(insertSql)
          counts.foreach {
            case (str, lng) =>
              insert.setString(1, str)
              insert.setLong(2, lng)
              insert.addBatch
          }
          val completed = insert.executeBatch
          conn.commit
          conn.close
          context.system.log.info(s"${completed.sum} rows inserted")
          completed.sum
      }
      cats.zip(rows).map { case (category, count) => s"${count} records written for ${category}!" }.mkString("\n")
    } catch {
      case e: Exception => e.getMessage()
    }
  }
}
