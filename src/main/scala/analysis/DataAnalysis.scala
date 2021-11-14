package analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object DataApp{
    def main(args: Array[String]): Unit = {
        //Replace the localPath variable with your preferred destination
        //Hortonworks SandBox
        val localPath = "file:///home/maria_dev/project2data/"
        //Begench S3
        //val localPath = "s3://alchemy-project-2/data/"
        //My Test S3
        //val localPath = "s3://project2datanalyzer/data/"

        val spark: SparkSession = SparkSession
            .builder()
            .master("local[1]")
            .appName("DataAnalyzer")
            .getOrCreate()
        val sc = spark.sparkContext

        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._

        val df = spark.read
            .option("header", true)
            .option("inferSchema", true)
            .csv(localPath + "real_estate_db.csv")


        getQuery4(df, localPath)
        getQuery8(df, localPath)
    }

    //Query the results for Question 4
    def getQuery4(df: DataFrame, localPath: String): Unit = {
        //Query the results by state
        //df.filter("rent_mean != 'NaN' AND family_mean != 'NaN'").groupBy("state").agg(avg("rent_mean"),avg("family_mean")).sort(asc("state")).show(52, false)
        df.filter("rent_mean != 'NaN' AND family_mean != 'NaN'").groupBy("state").agg(avg("rent_mean"),avg("family_mean")).sort(asc("state")).coalesce(1).write.mode("overwrite").csv(localPath + "Q4ByState")

        //Query the results by city
        //df.filter("rent_mean != 'NaN' AND family_mean != 'NaN'").groupBy("city").agg(avg("rent_mean"),avg("family_mean")).sort(asc("city")).show(10000, false)
        df.filter("rent_mean != 'NaN' AND family_mean != 'NaN'").groupBy("city").agg(avg("rent_mean"),avg("family_mean")).sort(asc("city")).coalesce(1).write.mode("overwrite").csv(localPath + "Q4ByCity")
    }

    //Query the results for Question 8
    def getQuery8(df: DataFrame, localPath: String): Unit = {
        //Query the results by state
        //df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("state").agg(avg("rent_mean"),avg("married")).sort(asc("state")).show(52, false)
        df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("state").agg(avg("rent_mean"),avg("married")).sort(asc("state")).coalesce(1).write.mode("overwrite").csv(localPath + "Q8ByState")

        //Query the results by city
        //df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("city").agg(avg("rent_mean"),avg("married")).sort(asc("city")).show(10000, false)
        df.filter("rent_mean != 'NaN' AND married != 'NaN'").groupBy("city").agg(avg("rent_mean"),avg("married")).sort(asc("city")).coalesce(1).write.mode("overwrite").csv(localPath + "Q8ByCity")
    }


}