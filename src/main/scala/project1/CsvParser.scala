package project1

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.LinkedHashMap

/** Reads in a csv file, parses header to allow for various operations
  *
  */
object CsvParser {
  sealed trait Counts
  case class Count(counts: Option[LinkedHashMap[String, Long]]) extends Counts
  case class Count2(counts: Option[LinkedHashMap[List[String], Long]]) extends Counts

  /** Prints results in human-readable format
    *
    * @param result
    */
  def prettyPrint(result: Counts): Unit =
    result match {
      case Count2(counts) =>
        counts foreach (_ map { case (lst, lng) => lst.mkString(", ") + ": " + lng.toString } foreach println)
      case Count(counts) => counts foreach (_ map { case (str, lng) => str + ": " + lng.toString } foreach println)
      case _ => println("Invalid results")
    }

  /** Returns result as one multi-line string
    *
    * @param result
    * @return
    */
  def asString(result: Counts): String =
    result match {
      case Count2(counts) =>
        counts
          .getOrElse(List())
          .map { case (lst, lng) => lst.mkString(", ") + ": " + lng.toString }
          .mkString("\n")
      case Count(counts) =>
        counts
          .getOrElse(List())
          .map { case (str, lng) => str + ": " + lng.toString }
          .mkString("\n")
      case _ => "Invalid results"
    }

}

/** Reads in a csv file, parses header to allow for various operations
  *
  * @param csvFile
  */
class CsvParser(val csvFile: String) extends SparkSessionWrapper {
  // read csv file
  val rdd = spark.sparkContext.textFile(csvFile)
  // split and trim
  val rows = rdd.map(line => line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1).map(_.trim))
  // get header
  val header = rows.first
  // filter out header
  val data = rows.filter(_(0) != header(0))
  // splits to map (header/value pairs)
  val maps = data.map(splits => header.zip(splits).toMap)

  import CsvParser._

  /** Returns list of counts of crimes for 1 category sorted in descending order
    *
    * @param category
    */
  def getCount(category: String): Count = {
    if (header.contains(category)) {
      val crimesByCat = maps.map(s => (s(category), s - category))
      val countsByCat = crimesByCat.countByKey
      val sortedCounts = LinkedHashMap(countsByCat.toSeq.sortWith(_._2 > _._2): _*)
      Count(Some(sortedCounts))
    } else
      Count(None)
  }

  /** Returns list of counts of crimes grouped by 2 categories sorted in descending order
    *
    * @param category1
    * @param category2
    */
  def get2Count(category1: String, category2: String): Count2 = {
    if (header.contains(category1) && header.contains(category2)) {
      val cats = List(category1, category2).sorted
      val crimesBy2Cat = maps.map(s => (List(s(cats(0)), s(cats(1))), s - cats(0) - cats(1)))
      val countsBy2Cat = crimesBy2Cat.countByKey
      val sorted2Counts = LinkedHashMap(countsBy2Cat.toSeq.sortWith(_._2 > _._2): _*)
      Count2(Some(sorted2Counts))
    } else
      Count2(None)
  }

  /** Returns list of counts of crimes grouped by 1 or 2 categories sorted in descending order
    *
    * @param category1
    * @param category2
    */
  def getCounts(categories: Seq[String]): LinkedHashMap[String, Long] = {
    val results = categories.length match {
      case 1 => getCount(categories(0))
      case _ => get2Count(categories(0), categories(1))
    }
    val counts = results match {
      case Count(results) => results.getOrElse(LinkedHashMap())
      case Count2(results) => results.getOrElse(LinkedHashMap()).map { case (lst, lng) => (lst.mkString(", "), lng) }
    }
    counts
  }
}

object SimpleJob {
  val CrimesParser = new CsvParser("Crimes2015.csv")
  // val counts = CrimesParser.getCount("Primary Type")
  val counts = CrimesParser.get2Count("Primary Type", "Location Description")
  // CsvParser.prettyPrint(counts)
  print(CsvParser.asString(counts))
}
