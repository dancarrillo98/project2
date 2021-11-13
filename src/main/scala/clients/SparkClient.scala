package clients

import models.Record
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Handles connection to Spark and CRUD operations
 *
 */
object SparkClient extends Client[Record] {

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

    def Create(item: Record): Unit = {

    }

    override def Read(): Record = {

        return null
    }

    override def Update(t: Record): Unit = {

    }

    override def Delete(t: Record): Unit = {

    }

    override def Close(): Unit = {

    }

}
