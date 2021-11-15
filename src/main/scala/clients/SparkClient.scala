package clients

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/** Handles connection to Spark and CRUD operations
 *
 */
object SparkClient extends Client[Row] {

    val session: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Demographics")
      .getOrCreate()

    val context: SparkContext = session.sparkContext

    var df: DataFrame = null

    override def Connect(): Unit = {

        df = session.read
          .option("header", true)
          .option("inferSchema", true)
          .option("delimiter", ",")
          .csv("/home/hdoop/Projects/project2/src/main/resources/real_estate_db.csv")

    }

    def Data(): DataFrame = df

    def Create(item: Row): Unit = {

    }

    override def Read(): Row = {

        return null
    }

    override def Update(t: Row): Unit = {

    }

    override def Delete(t: Row): Unit = {

    }

    override def Close(): Unit = {

    }

}
