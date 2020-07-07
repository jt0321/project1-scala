package project1

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

trait SparkSessionWrapper extends Serializable {
  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    SparkSession.builder().master("local[*]").appName("my cool app").getOrCreate()
  }
}
