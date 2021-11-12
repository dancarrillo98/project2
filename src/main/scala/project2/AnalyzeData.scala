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
       

        // Analyze data


        // Analysis 10.1: Create table for "Does a larger population mean higher rent?""
        val df10 = demographicsData.select("city", "state", "pop", "rent_mean")
        df10.createOrReplaceTempView("Rent_Pop")
        val df10Table = spark.sql("SELECT city AS City, state AS State, SUM(CAST(pop AS int)) AS Population, ROUND(AVG(CAST(rent_mean AS int)), 2) AS AverageRent FROM Rent_Pop GROUP BY city, state ORDER BY Population DESC")

        df10Table.coalesce(1)
            .write
            .option("header", "true")
            .csv(hdfsFilePath + "q10_Table")

        // Analysis 10.2: Calculate correlation coefficient r for 10.1
        df10Table.createOrReplaceTempView("Rent_Pop_Adjusted")
        val df10Coef = spark.sql("SELECT ROUND(((AVG(Population*AverageRent)-(AVG(Population)*AVG(AverageRent))) / (STD(Population)*STD(AverageRent))), 2) AS q10_coef_r FROM Rent_Pop_Adjusted")

            df10Coef.coalesce(1)
            .write
            .option("header", "true")
            .csv(hdfsFilePath + "q10_Coef")


    } // end main

} // end class