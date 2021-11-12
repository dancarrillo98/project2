package clients

import models.Record
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Handles connection to Spark and CRUD operations
 *
 */
object SparkClient extends Client[Record] {

    val session: SparkSession = SparkSession.builder().master("local").appName("Demographics").getOrCreate()
    val context: SparkContext = session.sparkContext

    override def Connect(): Unit = {

        val df: DataFrame = session.read
            .option("header", true)
            .option("inferSchema", true)
            .option("delimiter", ",")
            .csv("src/main/resources/archive/real_estate_db.csv")

    }

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
