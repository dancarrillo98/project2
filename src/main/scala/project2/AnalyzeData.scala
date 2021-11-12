package project2

// Spark Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object AnalyzeData {
    def main(args: Array[String]): Unit = {

        // Define path to csv file with data
        val hdfsFilePath = "/user/maria_dev/project2/"
        
        // Create SparkSession
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[3]")
            .appName("project2")
            .getOrCreate()
        
        import spark.implicits._
        
        // Create SparkContext
        val sc = spark.sparkContext

        // Read csv data into a DataFrame
        val demographicsData = spark.read
            .option("header", true)
            .option("inferSchema", true)
            .csv(hdfsFilePath + "real_estate_db.csv")
        
        // Show schema and first 20 rows to verify
        println("Printing Schema...")
        demographicsData.printSchema()
        // demographicsData.show()
       
        // Create Temp View for SQL queries

        // Analyze data
            // Analysis 1: Do high school degrees correlate with less debt
             //

            // Analysis 2: Do college degrees correlate with less debt


            // Analysis 3: Does a larger population mean higher rent
            val df10 = demographicsData.select(col("city"), col("state"), col("pop"), col("rent_mean"))
            df10.createOrReplaceTempView("Rent_Pop_table")
            val df10Result = spark.sql("SELECT city AS City, state AS State, SUM(CAST(pop AS int)) AS Population, ROUND(AVG(CAST(rent_mean AS int)), 2) AS AverageRent FROM Rent_Pop_table GROUP BY city, state ORDER BY Population DESC")

            df10Result.coalesce(1)
                .write
                .option("header", "true")
                .csv(hdfsFilePath + "q10_Result")

        // Store results of analysis to a file on HDFS (S3 later)


    } // end main

} // end class