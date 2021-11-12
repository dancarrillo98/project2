import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataApp{
    val localPath = "file:///home/maria_dev/project2data/real_estate_db.csv"
    val bucketPath = ""

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[3]")
            .appName("AjaySingala")
            .getOrCreate()
        val sc = spark.sparkContext

        val df = spark.read
            .option("header", true)
            .option("inferSchema", true)
            .csv(localPath)
            df.printSchema()
            df.show()
    }

}